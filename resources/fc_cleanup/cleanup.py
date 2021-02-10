"""Cleanup duplicate File Catalog records...

where the duplicate is indexed under /mnt/lfs*/.
"""


import argparse
import json
import logging
from typing import Any, cast, Dict, Generator, List, Tuple

import coloredlogs  # type: ignore[import]

# local imports
from rest_tools.client import RestClient  # type: ignore[import]

PAGE_SIZE = 10000

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
    metadata = rc.request_seq("GET", f"/api/files/{results[0]['uuid']}")

    return cast(FCEntry, metadata)


def remove_prefix(string: str, prefix: str) -> str:
    """Return string without the given prefix."""
    return string[len(prefix) :]


def _get_good_path(fpath: str) -> str:

    replacement_roots = [
        ("/mnt/lfs6/exp/", "/data/exp/"),
        ("/mnt/lfs6/sim/", "/data/sim/"),
    ]

    for bad_root, good_root in replacement_roots:
        if fpath.startswith(bad_root):
            return good_root + remove_prefix(fpath, bad_root)

    raise Exception(f"Unaccounted for prefix: {fpath}")


def _compatible_locations_values(
    evil_twin_locations: List[Dict[str, str]], good_twin_locations: List[Dict[str, str]]
) -> bool:
    # replace WIPAC-site path (these will differ b/c they are the logical_name)
    for locations in [evil_twin_locations, good_twin_locations]:
        for i in range(len(locations)):  # pylint: disable=C0200 # allow in-line changes
            if locations[i]["site"] == "WIPAC":
                locations[i]["path"] = "WIPAC-PLACEHOLDER"

    # are these the same?
    if evil_twin_locations == good_twin_locations:
        return True

    # does the evil twin have any locations that the good twin does not?
    # the good twin can have more locations--AKA it's been moved to NERSC
    for evil_locus in evil_twin_locations:
        if evil_locus not in good_twin_locations:
            return False

    return True


def _compare_fc_entries(
    evil_twin: FCEntry, good_twin: FCEntry, ignored_fields: List[str]
) -> bool:
    keys = set(list(evil_twin.keys()) + list(good_twin.keys()))

    for key in keys:
        if key in ignored_fields:
            continue
        if evil_twin[key] != good_twin[key]:
            logging.info(
                f"Field Mismatch: {key} ({evil_twin[key]} vs {good_twin[key]})"
            )
            return False

    return True


def find_twins(rc: RestClient, bad_rooted_fpath: str) -> Tuple[str, str]:
    """Get evil twin and good twin FC entries' uuids.

    If no twin (good or bad) is found, raise FileNotFoundError.
    """
    good_twin = _find_fc_entry(rc, _get_good_path(bad_rooted_fpath))
    good_twin_uuid = good_twin["uuid"]
    evil_twin = _find_fc_entry(rc, bad_rooted_fpath)
    evil_twin_uuid = evil_twin["uuid"]

    # compare "locations"-field
    if not _compatible_locations_values(evil_twin["locations"], good_twin["locations"]):
        raise Exception(
            f"Locations metadata not compatible: {evil_twin} vs {good_twin}"
        )

    ignored_fields = ["_links", "logical_name", "uuid", "locations"]
    if not _compare_fc_entries(evil_twin, good_twin, ignored_fields):
        raise Exception(
            f"These don't match {evil_twin} vs {good_twin} (disregarding: {ignored_fields})"
        )

    return evil_twin_uuid, good_twin_uuid


def bad_rooted_fc_fpaths(rc: RestClient) -> Generator[str, None, None]:
    """Yield each FC filepath rooted at /mnt/lfs*/.

    Search will be halted either by a REST error, or manually by the
    user.
    """
    previous_page: List[Dict[str, Any]] = []
    page = 0
    while True:
        logging.info(
            f"Looking for more bad-rooted paths (page={page}, limit={PAGE_SIZE})..."
        )

        # Query
        body = {"start": page * PAGE_SIZE, "limit": PAGE_SIZE}
        files = rc.request_seq("GET", "/api/files", body)["files"]
        if not files:
            logging.warning("No more files.")
            return
        if len(files) != PAGE_SIZE:
            logging.warning(f"Asked for {PAGE_SIZE} files, received {len(files)}")

        # Case 0: nothing was deleted from the bad-paths yield last time -> get next page
        if files == previous_page:
            logging.warning("This page is the same as the previous page.")
            page += 1
            continue

        previous_page = files
        bads = [
            f["logical_name"] for f in files if f["logical_name"].startswith("/mnt/lfs")
        ]

        # Case 1a: there are no bad paths -> get next page
        if not bads:
            # since there were no bad paths, we know nothing will be deleted
            logging.warning("No bad-rooted paths found in page.")
            page += 1
            continue

        # Case 1b: there *are* bad paths
        yield from bads


def delete_evil_twin_catalog_entries(rc: RestClient, dryrun: bool = False) -> int:
    """Delete each bad-rooted path FC entry (if each has a good twin)."""
    i = 0
    for i, bad_rooted_fpath in enumerate(bad_rooted_fc_fpaths(rc), start=1):
        logging.info(f"Bad path #{i}: {bad_rooted_fpath}")

        try:
            evil_twin_uuid, good_twin_uuid = find_twins(rc, bad_rooted_fpath)
            logging.info(
                f"Found: good-twin={good_twin_uuid} evil-twin={evil_twin_uuid}"
            )
        except FileNotFoundError:
            logging.warning("No good twin found.")
            continue

        if dryrun:
            logging.warning(
                f"Dry-Run Enabled: Not DELETE'ing File Catalog entry! i={i}"
            )
        else:
            rc.request_seq("DELETE", f"/api/files/{evil_twin_uuid}")
            logging.info(f"Deleted: {i}")

    return i


def main() -> None:
    """Do Main."""
    coloredlogs.install(level=logging.INFO)

    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="do everything except deleting File Catalog entries. "
        "NOTE: since the FC will remain the same size, "
        '"GET" @ "/api/files" will continue to return the same entries.',
    )
    args = parser.parse_args()

    rc = RestClient("https://file-catalog.icecube.wisc.edu/", token=args.token)

    # Go
    total_deleted = delete_evil_twin_catalog_entries(rc, args.dryrun)
    if not total_deleted:
        raise Exception("No FC entries found/deleted")
    else:
        logging.info(f"Total Deleted: {total_deleted}")


if __name__ == "__main__":
    main()
