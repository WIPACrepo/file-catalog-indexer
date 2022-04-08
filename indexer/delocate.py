"""For each filepath, remove the matching location entry from its File Catalog record."""


import argparse
import asyncio
import json
import logging
import os
from typing import Dict, List, Tuple, cast

import coloredlogs  # type: ignore[import]
import requests
from rest_tools.client import RestClient

from indexer.utils import file_utils


def file_does_not_exist(fpath: str) -> None:
    """Raise `FileExistsError` is the filepath exists."""
    if os.path.exists(fpath):
        raise FileExistsError(
            f"Filepath `{fpath}` exists; can only de-locate already FS-deleted filepaths"
        )


class FCRecordNotFoundError(Exception):
    """Raised when a File Catalog record is not found."""


class Location:
    """Represent a location object."""

    def __init__(self, fpath: str, site: str) -> None:
        self.fpath = fpath
        self.site = site

    def __str__(self) -> str:
        """Get string for printing."""
        return f"(fpath={self.fpath}, site={self.site})"

    def to_dict(self) -> Dict[str, str]:
        """Convert location to dict for REST querying."""
        return {"site": self.site, "path": self.fpath}


async def get_uuid(location: Location, rc: RestClient) -> str:
    """Grab the matching FC record's uuid."""
    response = await rc.request(
        "GET",
        "/api/files",
        {"query": json.dumps({"locations": {"$elemMatch": location.to_dict()}})},
    )
    try:
        return cast(str, response["files"][0]["uuid"])
    except (KeyError, IndexError) as e:
        raise FCRecordNotFoundError("There's no matching location entry in FC") from e


async def remove_location(location: Location, rc: RestClient, uuid: str) -> None:
    """Remove the fpath from the record at uuid."""
    response = await rc.request(
        "POST",
        f"/api/files/{uuid}/actions/remove_location",
        location.to_dict(),
    )
    if not response:
        logging.info(f"Removed Entire Record: uuid={uuid}, {location}")
    else:
        logging.info(f"Removed Location: uuid={uuid}, {location}")


async def delocate_filepaths(
    fpath_queue: List[str], rc: RestClient, site: str, skip_missing_locations: bool
) -> Tuple[int, int, int]:
    """De-locate all the filepaths in the queue."""
    delocated = 0
    skipped = 0
    already_deleted = 0

    for fpath in fpath_queue:
        file_does_not_exist(fpath)  # point of no-return so do this again
        location = Location(fpath, site)
        logging.info(f"De-locating: {location}")

        # Grab uuid
        try:
            uuid = await get_uuid(location, rc)
            logging.info(f"Found uuid: {uuid}, {location}")
        except FCRecordNotFoundError as e:
            if skip_missing_locations:
                logging.warning(f"Skipping Location, {uuid}, {location}: {str(e)}")
                skipped += 1
                continue
            raise

        # Remove location
        try:
            await remove_location(location, rc, uuid)
            delocated += 1
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logging.warning(
                    f"Skipping Record {uuid}, {location}: Record already deleted"
                )
                already_deleted += 1
                continue
            raise

    return delocated, skipped, already_deleted


def main() -> None:
    """Traverse paths, recursively, and print out metadata."""
    parser = argparse.ArgumentParser(
        description="Find files under PATH(s), for each, remove the matching location "
        "entry from its File Catalog record.",
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "paths", metavar="PATHS", nargs="*", help="filepath(s) to de-locate"
    )
    parser.add_argument(
        "-f",
        "--paths-file",
        default=None,
        help="new-line-delimited text file containing filepath(s) to de-locate "
        "(use this option for a large number of paths)",
    )
    parser.add_argument(
        "-s", "--site", required=True, help='site value of the "locations" object'
    )
    parser.add_argument(
        "-t", "--token", required=True, help="REST token for File Catalog"
    )
    parser.add_argument(
        "--skip-missing-locations",
        default=False,
        action="store_true",
        help="don't exit when a filepath already isn't in the File Catalog",
    )
    parser.add_argument("-l", "--log", default="INFO", help="the output logging level")

    # grab args
    args = parser.parse_args()
    coloredlogs.install(level=args.log)
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    # aggregate filepaths & make sure none exist
    paths = file_utils.sorted_unique_filepaths(
        file_of_filepaths=args.paths_file, list_of_filepaths=args.paths, abspaths=False
    )
    for fpath in paths:
        file_does_not_exist(fpath)

    # de-locate
    rc = RestClient("https://file-catalog.icecube.wisc.edu/", token=args.token)
    delocated, skipped, already_deleted = asyncio.get_event_loop().run_until_complete(
        delocate_filepaths(paths, rc, args.site, args.skip_missing_locations)
    )

    logging.info("--------------------------------------")
    logging.info(f"De-located Locations    = {delocated} ")
    logging.info(
        f"Skipped Locations       = {skipped} "
        f"(--skip-missing-locations was {'' if args.skip_missing_locations else 'NOT'} included)"
    )
    logging.info(f"Already-Deleted Records = {already_deleted} ")
    logging.info("Done.")


if __name__ == "__main__":
    main()
