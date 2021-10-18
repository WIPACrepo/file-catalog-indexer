"""For each filepath, remove the matching location entry from its File Catalog record."""


import argparse
import asyncio
import json
import logging
import os
from typing import List

import coloredlogs  # type: ignore[import]
from rest_tools.client import RestClient


class FCRecordNotFoundError(Exception):
    """Raised when a File Catalog record is not found."""


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
        raise FCRecordNotFoundError(
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


async def delocate_filepaths(fpath_queue: List[str], rc: RestClient, site: str) -> None:
    """De-locate all the filepaths in the queue."""
    for fpath in fpath_queue:
        if os.path.exists(fpath):
            raise FileExistsError(
                f"Filepath `{fpath}` exists; can only de-locate already FS-deleted filepaths"
            )

        logging.info(f"De-locating File: {fpath}")
        await delocate(fpath, rc, site)


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
    asyncio.get_event_loop().run_until_complete(
        delocate_filepaths(args.paths, rc, args.site)
    )


if __name__ == "__main__":
    main()
