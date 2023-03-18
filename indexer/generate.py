#!/usr/bin/env python3
# generate.py
"""Generate metadata for file(s) (no communication with File Catalog)."""

import argparse
import logging
import os
import pprint

from wipac_dev_tools import logging_tools

from indexer import defaults
from indexer.client_auth import (
    add_auth_to_argparse,
    create_oauth_config,
    create_rest_config,
)
from indexer.config import IndexerConfiguration
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
    parser.add_argument("paths", metavar="PATHS", nargs="+", help="path(s) to scan for files.")
    parser.add_argument("--basic-only", default=False, action="store_true", help="only collect basic metadata")
    parser.add_argument("--iceprodv1-db-pass", default="", help="IceProd1 SQL password")
    parser.add_argument("-l", "--log", default="INFO", help="the output logging level")
    parser.add_argument("-s", "--site", required=True, help='site value of the "locations" object')
    add_auth_to_argparse(parser)
    args = parser.parse_args()

    # do some logging
    logging_tools.set_level(args.log, use_coloredlogs=True)
    logging_tools.log_argparse_args(args)

    index_config: IndexerConfiguration = {
        "basic_only": args.basic_only,
        "denylist": defaults.DENYLIST,
        "denylist_file": defaults.DENYLIST_FILE,
        "dryrun": False,
        "iceprodv1_db_pass": args.iceprodv1_db_pass,
        "n_processes": defaults.N_PROCESSES,
        "non_recursive": False,
        "patch": False,
        "paths": args.paths,
        "paths_file": defaults.PATHS_FILE,
        "site": args.site,
    }
    oauth_config = create_oauth_config(args)
    rest_config = create_rest_config(args)
    manager = MetadataManager(index_config, oauth_config, rest_config)

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
