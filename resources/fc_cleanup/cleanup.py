"""Cleanup duplicate File Catalog records...

where the duplicate is indexed under the file's realpath.
"""


import argparse
import json
import logging
import os
from typing import Any, cast, Dict, Generator, Tuple

import coloredlogs  # type: ignore[import]

# local imports
from rest_tools.client import RestClient  # type: ignore[import]

BATCH_SIZE = 10000

FCFile = Dict[str, Any]


def _get_fcfile(rc: RestClient, logical_name: str) -> FCFile:
    body = {
        "query": json.dumps({"logical_name": logical_name}),
        "keys": "logical_name|checksum",
    }

    try:
        results = rc.request_seq("GET", "/api/files", body)["files"]
    except KeyError:
        raise FileNotFoundError
    if not results:
        raise FileNotFoundError
    if len(results) > 1:
        raise Exception(f"Multiple FC matches for {logical_name}")

    return cast(FCFile, results[0])


def get_evil_twin(rc: RestClient, this: FCFile) -> FCFile:
    """Get the FC file that is indexed under `this`'s realpath."""
    realpath = os.path.realpath(this["logical_name"])
    evil_twin = _get_fcfile(rc, realpath)

    # TODO - what other fields to check?
    if this["checksum"]["sha512"] != evil_twin["checksum"]["sha512"]:
        raise Exception(
            f"Checksums don't match {this['logical_name']} vs {evil_twin['logical_name']}"
        )

    return evil_twin


def get_fcfile_and_evil_twin(
    fpaths_dump: str, rc: RestClient
) -> Generator[Tuple[FCFile, FCFile], None, None]:
    """Yield each file and its evil twin (rooted at /mnt/)."""
    with open(fpaths_dump) as f_dump:
        for line in f_dump:
            logical_name = line.strip()
            fcfile = _get_fcfile(rc, logical_name)
            try:
                evil_twin = get_evil_twin(rc, fcfile)
            except FileNotFoundError:
                continue
            yield fcfile, evil_twin


def main() -> None:
    """Do Main."""
    coloredlogs.install(level=logging.INFO)

    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    parser.add_argument(
        "--files-to-check",
        required=True,
        help=r"file containing filepaths to check; each line is: `<logical_name>\n`",
    )
    args = parser.parse_args()

    rc = RestClient("https://file-catalog.icecube.wisc.edu/", token=args.token)

    # Go
    total = 0
    for fcfile, evil_twin in get_fcfile_and_evil_twin(args.files_to_check, rc):
        logging.info(f'Found {fcfile["logical_name"]} and {evil_twin["logical_name"]}')
        # delete evil twin
        rc.request_seq("GET", f"/api/files{evil_twin['uuid']}")
        total += 1

    logging.info(f"Total deleted: {total}")


if __name__ == "__main__":
    main()
