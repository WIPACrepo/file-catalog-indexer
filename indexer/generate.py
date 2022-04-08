"""Generate metadata for file(s) (no communication with File Catalog)."""


import argparse
import logging
import os
import pprint

import coloredlogs  # type: ignore[import]

from indexer.metadata_manager import MetadataManager
from indexer.utils import file_utils


def main() -> None:
    """Traverse paths, recursively, and print out metadata."""
    parser = argparse.ArgumentParser(
        description="Find files under PATH(s), compute their metadata and "
        "print it. (No communication with File Catalog)",
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
        "--basic-only",
        default=False,
        action="store_true",
        help="only collect basic metadata",
    )
    parser.add_argument("--iceprodv2-rc-token", default="", help="IceProd2 REST token")
    parser.add_argument("--iceprodv1-db-pass", default="", help="IceProd1 SQL password")
    parser.add_argument("-l", "--log", default="INFO", help="the output logging level")

    args = parser.parse_args()
    coloredlogs.install(level=args.log)
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    manager = MetadataManager(
        args.site,
        basic_only=args.basic_only,
        iceprodv2_rc_token=args.iceprodv2_rc_token,
        iceprodv1_db_pass=args.iceprodv1_db_pass,
    )

    filepath_queue = [os.path.abspath(p) for p in args.paths]

    while filepath_queue:
        fpath = filepath_queue.pop(0)
        if not file_utils.is_processable_path(fpath):  # pylint: disable=R1724
            logging.warning(f"File is not processable: {fpath}")
            continue
        elif os.path.isfile(fpath):
            logging.info(f"Generating metadata for file: {fpath}")
            metadata = manager.new_file(fpath).generate()
            pprint.pprint(metadata)
        elif os.path.isdir(fpath):
            logging.info(f"Appending directory's contents to queue: {fpath}")
            filepath_queue.extend(file_utils.get_subpaths(fpath))
        else:
            raise Exception(f"Unaccounted for file type: {fpath}")


if __name__ == "__main__":
    main()
