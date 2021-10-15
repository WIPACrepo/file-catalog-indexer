"""For each filepath, remove the matching location entry from its File Catalog record."""


import argparse
import logging
import os
from typing import List

import coloredlogs  # type: ignore[import]

# local imports
import file_utils


class FileNotProcessableError(Exception):
    """Raised for non-processable filepaths."""


def delocate(fpath: str) -> None:
    pass


def recursively_delocate_filepaths(filepath_queue: List[str]) -> None:
    """De-locate all the files starting with those in the queue, recursively."""
    while filepath_queue:
        fpath = filepath_queue.pop(0)
        # Is this a processable path?
        if not file_utils.is_processable_path(fpath):  # pylint: disable=R1724
            raise FileNotProcessableError(f"File is not processable: {fpath}")
        # Is this even a file?
        elif os.path.isfile(fpath):
            logging.info(f"Generating metadata for file: {fpath}")
            delocate(fpath)
            continue
        # Well, is it a directory?
        elif os.path.isdir(fpath):
            logging.info(f"Appending directory's contents to queue: {fpath}")
            filepath_queue.extend(file_utils.get_subpaths(fpath))
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

    recursively_delocate_filepaths([os.path.abspath(p) for p in args.paths])


if __name__ == "__main__":
    main()
