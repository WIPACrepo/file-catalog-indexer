"""Cleanup duplicate File Catalog records...

where the duplicate is indexed under /mnt/lfs*/.
"""


import argparse
import json
import logging
from typing import Any, cast, Dict, Generator, Tuple

import coloredlogs  # type: ignore[import]

# local imports
from rest_tools.client import RestClient  # type: ignore[import]

BATCH_SIZE = 10000

FCEntry = Dict[str, Any]


def _find_fc_entry(rc: RestClient, logical_name: str) -> FCEntry:

    # get uuid
    try:
        body = {"query": json.dumps({"logical_name": logical_name})}
        results = rc.request_seq("GET", "/api/files", body)["files"]
    except KeyError:
        raise FileNotFoundError
    if not results:
        raise FileNotFoundError
    if len(results) > 1:
        raise Exception(f"Multiple FC matches for {logical_name}")

    # get full metadata
    results = rc.request_seq("GET", f"/api/files/{results[0]['uuid']}")

    return cast(FCEntry, results[0])


def _get_good_path(bad_path: str) -> str:
    def strip_prefix(string: str, prefix: str) -> str:
        return string[len(prefix) :]

    if bad_path.startswith("/mnt/lfs6/exp/"):
        return "/data/exp/" + strip_prefix(bad_path, "/mnt/lfs6/exp/")

    elif bad_path.startswith("/mnt/lfs6/sim/"):
        return "/data/sim/" + strip_prefix(bad_path, "/mnt/lfs6/exp/")

    raise Exception(f"Unaccounted for prefix: {bad_path}")


def find_twins(rc: RestClient, bad_rooted_fpath: str) -> Tuple[FCEntry, FCEntry]:
    """Get the FC entry that is indexed under `evil_twin`'s canonical path."""
    # first, try to find entry w/ good path (otherwise, no point to continue)
    good_twin = _find_fc_entry(rc, _get_good_path(bad_rooted_fpath))
    evil_twin = _find_fc_entry(rc, bad_rooted_fpath)

    # ignore keys that aren't expected to match
    # TODO - what other fields to ignore?
    ignored_fields = ["_links", "logical_name", "uuid"]

    def copy_for_compare(entry: FCEntry) -> FCEntry:
        xerox = entry.copy()
        for field in ignored_fields:
            del xerox[field]
        # replace logical_name in locations object with a placeholder value
        for i, locus in enumerate(xerox["locations"].copy()):
            if locus["path"] == entry["logical_name"]:
                xerox["locations"][i]["path"] = "PLACEHOLDER"
        return xerox

    if copy_for_compare(evil_twin) != copy_for_compare(good_twin):
        raise Exception(
            f"These don't match {evil_twin} vs {good_twin} (disregarding: {ignored_fields})"
        )

    return evil_twin, good_twin


def bad_rooted_fc_fpaths(rc: RestClient) -> Generator[str, None, None]:
    """Yield each FC filepath rooted at /mnt/lfs*/."""
    files = rc.request_seq("GET", "/api/files")["files"]

    bads = [
        f["logical_name"] for f in files if f["logical_name"].startswith("/mnt/lfs")
    ]

    yield from bads


def get_evil_twin_catalog_entries(rc: RestClient) -> Generator[FCEntry, None, None]:
    """Yield each FC file and its good twin (rooted at /data/)."""
    for bad_rooted_fpath in bad_rooted_fc_fpaths(rc):
        try:
            evil_twin, good_twin = find_twins(rc, bad_rooted_fpath)
            logging.info(
                f'Found {evil_twin["logical_name"]} and {good_twin["logical_name"]}'
            )
        except FileNotFoundError:
            continue
        yield evil_twin


def main() -> None:
    """Do Main."""
    coloredlogs.install(level=logging.INFO)

    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    args = parser.parse_args()

    rc = RestClient("https://file-catalog.icecube.wisc.edu/", token=args.token)

    # Go
    deleted = False
    for i, evil_twin in enumerate(get_evil_twin_catalog_entries(rc)):
        deleted = True
        rc.request_seq("DELETE", f"/api/files/{evil_twin['uuid']}")
        logging.info(f"Deleted: {i}")

    if not deleted:
        raise Exception("No FC entries found/deleted")


if __name__ == "__main__":
    main()
