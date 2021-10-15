"""For each filepath, remove the matching location entry from its File Catalog record."""


import argparse
import json
import logging
import os
from typing import List

import coloredlogs  # type: ignore[import]
from rest_tools.client import RestClient

# local imports
import file_utils


class FileNotProcessableError(Exception):
    """Raised for non-processable filepaths."""


async def delocate(fpath: str, rc: RestClient, site: str) -> None:
    """Remove the fpath from a matching FC record."""
    response = await rc.request(
        "GET",
        "/api/files",
        {"query": json.dumps({"locations.path": fpath})},
    )
    try:
        uuid = response["files"][0]["uuid"]
    except KeyError as e:
        raise FileNotFoundError(
            f"There's no matching location entry in FC for `{fpath}`"
        ) from e

    # De-locate
    response = await rc.request(
        "POST",
        f"/api/files/{uuid}/actions/remove_location",
        {"site": site, "path": fpath},
    )
    if not response:
        logging.info(f"Removed Entire Record: uuid={uuid}, fpath={fpath}")
    else:
        logging.info(f"Removed Location: uuid={uuid}, fpath={fpath}")


def recursively_delocate_filepaths(
    fpath_queue: List[str], rc: RestClient, site: str
) -> None:
    """De-locate all the files starting with those in the queue, recursively."""
    while fpath_queue:
        fpath = fpath_queue.pop(0)
        # Is this a processable path?
        if not file_utils.is_processable_path(fpath):  # pylint: disable=R1724
            raise FileNotProcessableError(f"File is not processable: {fpath}")
        # Is this even a file?
        elif os.path.isfile(fpath):
            logging.info(f"De-locating File: {fpath}")
            delocate(fpath, rc, site)
            continue
        # Well, is it a directory?
        elif os.path.isdir(fpath):
            logging.debug(f"Appending directory's contents to queue: {fpath}")
            fpath_queue.extend(file_utils.get_subpaths(fpath))
        # Who knows what this is...
        else:
            raise FileNotProcessableError(f"Unaccounted for file type: {fpath}")


def main() -> None:
    """Traverse paths, recursively, and print out metadata."""
    parser = argparse.ArgumentParser(
        description="Find files under PATH(s), for each, remove the matching location "
        "entry from its File Catalog record.",
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "paths", metavar="PATHS", nargs="+", help="path(s) to scan for files."
    )
    parser.add_argument(
        "-s", "--site", required=True, help='site value of the "locations" object'
    )
    parser.add_argument(
        "-t", "--token", required=True, help="REST token for File Catalog"
    )
    parser.add_argument("-l", "--log", default="INFO", help="the output logging level")

    args = parser.parse_args()
    coloredlogs.install(level=args.log)
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    rc = RestClient(args.url, token=args.token)

    recursively_delocate_filepaths(
        [os.path.abspath(p) for p in args.paths], rc, args.site
    )


if __name__ == "__main__":
    main()
