#!/usr/bin/env python3
# get_nonindexed_files.py
"""Utility to get files that have not been indexed from a traverse file."""

import argparse
import concurrent.futures
import json
import logging
from typing import List

import coloredlogs  # type: ignore[import]
import more_itertools as mit
from rest_tools.client import RestClient

from indexer.client_auth import (
    add_auth_to_argparse,
    create_file_catalog_rest_client,
    create_oauth_config,
    create_rest_config,
)


def _check_fpaths(fpaths: List[str], rc: RestClient, thread_id: int) -> List[str]:
    # scan
    nonindexed_fpaths: List[str] = []
    for i, fpath in enumerate(fpaths, start=1):
        if i % 100000 == 1:
            logging.warning(
                f"thread-{thread_id} processed total: {i} (found {len(nonindexed_fpaths)} non-indexed)"
            )
        logging.info(f"#{i}")
        logging.debug(f"Looking at {fpath}")
        result = rc.request_seq(
            "GET",
            "/api/files",
            {
                "logical_name": fpath,  # filepath may exist as multiple logical_names
                "query": json.dumps({"locations.path": fpath}),
            },
        )
        if result["files"]:
            logging.debug("file is already indexed")
            continue
        logging.info("file is *not* indexed -> appending to list")
        nonindexed_fpaths.append(fpath)

    logging.warning(
        f"Thread-{thread_id} found {len(nonindexed_fpaths)} non-indexed filepaths."
    )
    return nonindexed_fpaths


def _split_up_infile(trav_file: str, npieces: int) -> List[List[str]]:
    logging.warning(f"Splitting up {trav_file} into {npieces} pieces")

    fpaths = [ln.strip() for ln in open(trav_file)]

    return [list(c) for c in mit.divide(npieces, fpaths)]


def main() -> None:
    """Do main."""
    # args
    parser = argparse.ArgumentParser(
        description="Get files that have not been indexed from a traverse file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--traverse-file",
        required=True,
        help="traverse file containing superset of filepaths",
    )
    parser.add_argument(
        "-l", "--log",
        default="DEBUG",
        help="the output logging level"
    )
    parser.add_argument(
        "--threads",
        required=True,
        type=int,
        help="# of threads"
    )
    add_auth_to_argparse(parser)
    args = parser.parse_args()

    # rest client
    oauth_config = create_oauth_config(args)
    rest_config = create_rest_config(args)
    rc: RestClient = create_file_catalog_rest_client(oauth_config, rest_config)

    # logging
    coloredlogs.install(level=args.log.upper())
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    # split up in-file
    fpath_chunks = _split_up_infile(args.traverse_file, args.threads)

    # spawn threads
    workers: List[concurrent.futures.Future[List[str]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as pool:
        logging.warning(f"Spinning off thread jobs ({args.threads})")
        workers.extend(
            pool.submit(_check_fpaths, c, rc, i)
            for i, c in enumerate(fpath_chunks)
        )

    # collect
    nonindexed_fpaths = []
    for worker in concurrent.futures.as_completed(workers):
        result_fpaths = worker.result()
        nonindexed_fpaths.extend(result_fpaths)
        logging.warning(
            f"Appending {len(result_fpaths)} non-indexed filepaths; now {len(nonindexed_fpaths)} total"
        )

    # print
    logging.warning(f"Found {len(nonindexed_fpaths)} non-indexed filepaths.")
    for fpath in nonindexed_fpaths:
        print(fpath)

    logging.warning("All done.")


if __name__ == "__main__":
    main()
