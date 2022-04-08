"""Run the indexer as a script."""

import argparse
import logging

import coloredlogs  # type: ignore[import]

from indexer import defaults, indexer

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find files under PATH(s), compute their metadata and "
        "upload it to File Catalog.",
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "paths", metavar="PATHS", nargs="*", help="path(s) to scan for files."
    )
    parser.add_argument(
        "-f",
        "--paths-file",
        default=defaults.PATHS_FILE,
        help="new-line-delimited text file containing path(s) to scan for files. "
        "(use this option for a large number of paths)",
    )
    parser.add_argument(
        "-n",
        "--non-recursive",
        default=False,
        action="store_true",
        help="do not recursively index / do not descend into subdirectories",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=defaults.N_PROCESSES,
        help="number of processes for multi-processing "
        "(ignored if using --non-recursive)",
    )
    parser.add_argument(
        "-u",
        "--url",
        default=defaults.URL,
        help="File Catalog URL",
    )
    parser.add_argument(
        "-s", "--site", required=True, help='site value of the "locations" object'
    )
    parser.add_argument(
        "-t", "--token", required=True, help="REST token for File Catalog"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=defaults.TIMEOUT,
        help="timeout duration (seconds) for File Catalog REST requests",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=defaults.RETRIES,
        help="number of retries for File Catalog REST requests",
    )
    parser.add_argument(
        "--basic-only",
        default=False,
        action="store_true",
        help="only collect basic metadata",
    )
    parser.add_argument(
        "--patch",
        default=False,
        action="store_true",
        help="replace/overwrite any existing File-Catalog entries (aka patch)",
    )
    parser.add_argument(
        "--blacklist",
        metavar="BLACKPATH",
        nargs="+",
        default=defaults.BLACKLIST,
        help="list of blacklisted filepaths; Ex: /foo/bar/ will skip /foo/bar/*",
    )
    parser.add_argument(
        "--blacklist-file",
        default=defaults.BLACKLIST_FILE,
        help="a file containing blacklisted filepaths on each line "
        "(this is a useful alternative to `--blacklist` when there's many blacklisted paths); "
        "Ex: /foo/bar/ will skip /foo/bar/*",
    )
    parser.add_argument(
        "-l",
        "--log",
        default="INFO",
        help="the output logging level",
    )
    parser.add_argument(
        "--iceprodv2-rc-token",
        default=defaults.ICEPRODV2_RC_TOKEN,
        help="IceProd2 REST token",
    )
    parser.add_argument(
        "--iceprodv1-db-pass",
        default=defaults.ICEPRODV1_DB_PASS,
        help="IceProd1 SQL password",
    )
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="do everything except POSTing/PATCHing to the File Catalog",
    )

    args = parser.parse_args()
    coloredlogs.install(level=args.log.upper())
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    indexer.index(
        token=args.token,
        site=args.site,
        paths=args.paths,
        paths_file=args.paths_file,
        blacklist=args.blacklist,
        blacklist_file=args.blacklist_file,
        url=args.url,
        timeout=args.token,
        retries=args.retries,
        basic_only=args.basic_only,
        patch=args.patch,
        iceprodv2_rc_token=args.iceprodv2_rc_token,
        iceprodv1_db_pass=args.iceprodv1_db_pass,
        dryrun=args.dryrun,
        non_recursive=args.non_recursive,
        processes=args.processes,
    )
