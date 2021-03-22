"""Utility to get files that have not been indexed from a traverse file."""


import argparse
import logging

import coloredlogs  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

# args
parser = argparse.ArgumentParser(
    description="Submit HTCondor DAGMan jobs for bulk indexing files for the File Catalog",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--traverse-file",
    required=True,
    help="traverse file containing superset of filepaths",
)
parser.add_argument("-l", "--log", default="DEBUG", help="the output logging level")
parser.add_argument("-t", "--token", required=True, help="REST token for File Catalog")
args = parser.parse_args()

# logging
args = parser.parse_args()
coloredlogs.install(level=args.log.upper())
for arg, val in vars(args).items():
    logging.warning(f"{arg}: {val}")

# setup
rc = RestClient("https://file-catalog.icecube.wisc.edu/", token=args.token)

# scan
NON_INDEXED = "nonindexed.paths"
with open(args.traverse_file) as f, open(NON_INDEXED, "w") as nonindexed_f:
    for i, line in enumerate(f, start=1):
        logging.info(f"#{i}")
        fpath = line.strip()
        logging.debug(f"Looking at {fpath}")
        result = rc.request_seq("GET", "/api/files", {"path": fpath})["files"]
        if result:
            logging.debug("file is already indexed")
            continue
        logging.info(f"file is *not* indexed -> appending to {NON_INDEXED}")
        print(fpath, file=nonindexed_f)
