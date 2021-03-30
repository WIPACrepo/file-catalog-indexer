"""Utility to get files that have not been indexed from a traverse file."""


import argparse
import concurrent.futures
import logging
from typing import List

import coloredlogs  # type: ignore[import]
import more_itertools as mit  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]


def _check_fpaths(fpaths: List[str], token: str, thread_id: int) -> List[str]:
    # setup
    rc = RestClient(
        "https://file-catalog.icecube.wisc.edu/",
        token=token,
        timeout=60 * 60,  # 1 hour
        retries=24,  # 1 day
    )

    # scan
    nonindexed_fpaths = []
    for i, fpath in enumerate(fpaths, start=1):
        if i % 100000 == 1:
            logging.warning(
                f"thread-{thread_id} processed total: {i} (found {len(nonindexed_fpaths)} non-indexed)"
            )
        logging.info(f"#{i}")
        logging.debug(f"Looking at {fpath}")
        result = rc.request_seq("GET", "/api/files", {"path": fpath})["files"]
        if result:
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
    parser.add_argument("-l", "--log", default="DEBUG", help="the output logging level")
    parser.add_argument(
        "-t", "--token", required=True, help="REST token for File Catalog"
    )
    parser.add_argument("--threads", required=True, type=int, help="# of threads")
    args = parser.parse_args()

    # logging
    args = parser.parse_args()
    coloredlogs.install(level=args.log.upper())
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    # split up in-file
    fpath_chunks = _split_up_infile(args.traverse_file, args.threads)

    # spawn threads
    workers: List[concurrent.futures.Future] = []  # type: ignore[type-arg]
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.threads) as pool:
        logging.warning(f"Spinning off thread jobs ({args.threads})")
        workers.extend(
            pool.submit(_check_fpaths, c, args.token, i)
            for i, c in enumerate(fpath_chunks)
        )

    # collect
    nonindexed_fpaths = []
    for worker in concurrent.futures.as_completed(workers):
        nonindexed_fpaths.extend(worker.result())

    # print
    logging.warning(f"Found {len(nonindexed_fpaths)} non-indexed filepaths.")
    for fpath in nonindexed_fpaths:
        print(fpath)


if __name__ == "__main__":
    main()
