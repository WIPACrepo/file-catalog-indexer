"""Print all the file catalog entries with a given filepath root."""


import argparse
import itertools
import logging

import coloredlogs  # type: ignore[import]

# local imports
from rest_tools.client import RestClient  # type: ignore[import]

BATCH_SIZE = 10000


def find_all_paths(rc: RestClient, root: str, outfile: str, start: int = 0) -> int:
    """Find all uuids for entry's with `'logical_name'`s starting w/ `root`."""
    total = 0
    for i in itertools.count(start):
        found_files = []
        logging.info(f"i={i} ({i*BATCH_SIZE})")
        files = rc.request_seq("GET", "/api/files", {"start": i * BATCH_SIZE})["files"]
        if not files:
            raise Exception("no files in response")
        for fcfile in files:
            if fcfile["logical_name"].startswith(root):
                found_files.append(fcfile["uuid"])
        logging.info(f"new files found: {len(found_files)}")
        if not found_files:
            continue
        total += len(found_files)
        logging.info(f"total files: {total} ({(total/((i+1)*BATCH_SIZE))*100:.2f}%)")
        with open(outfile, "a+") as out_f:
            for b in found_files:  # pylint: disable=C0103
                print(f"{b}", file=out_f)
    return total


if __name__ == "__main__":
    coloredlogs.install(level=logging.INFO)

    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    parser.add_argument("--outfile", required=True, help="dest to print out filepaths")
    parser.add_argument("--start", type=int, default=0, help="starting batch index")
    parser.add_argument("--root", default="/mnt/", help="filepath root to search for")
    args = parser.parse_args()

    # Go
    TOTAL = find_all_paths(
        RestClient(
            "https://file-catalog.icecube.wisc.edu/", token=args.token, retries=100,
        ),
        args.root,
        args.outfile,
        args.start,
    )
    logging.info(f"Total paths starting with {args.root}: {TOTAL}")
