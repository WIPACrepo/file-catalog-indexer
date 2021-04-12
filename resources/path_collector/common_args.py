"""Common argparse args.

Here to avoid copy-paste mistakes.
"""

import argparse
import os
from typing import List, Optional

import bitmath  # type: ignore[import]


def _parse_to_bytes(size: str) -> int:
    return int(bitmath.parse_string_unsafe(size).to_Byte())


def get_full_path(path: str) -> str:
    """Check that the path exists and return the full path."""
    if not path:
        return path

    full_path = os.path.abspath(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(full_path)

    return full_path


def get_parser_w_common_args(
    description: str, only: Optional[List[str]] = None
) -> argparse.ArgumentParser:
    """Get the parser with a few common arguments already added.

    Arguments:
        description {str} -- description for the ArgumentParser

    Keyword Arguments:
        only {Optional[List[str]]} -- an exclusive subset of the common arguments to add (default: {None})

    Returns:
        argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        description=description,
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # args
    if (not only) or ("traverse_root" in only):
        parser.add_argument(
            "traverse_root",
            help="root directory to traverse for files."
            " **Potentially bypassed if also using --fast-forward**",
            type=get_full_path,
        )
    if (not only) or ("--previous-traverse" in only):
        parser.add_argument(
            "--previous-traverse",
            type=get_full_path,
            help="prior file with file paths, eg: /data/user/eevans/data-exp-2020-03-10T15:11:42."
            " These files will be skipped.",
        )
    if (not only) or ("--exclude" in only):
        parser.add_argument(
            "--exclude",
            "-e",
            nargs="*",
            default=[],
            type=get_full_path,
            help="directories/paths to exclude from the traverse -- keep it short."
            " **Potentially bypassed if also using --fast-forward**",
        )
    if (not only) or ("--chunk-size" in only):
        parser.add_argument(
            "--chunk-size",
            type=_parse_to_bytes,
            default="200GB",
            help="aggregate file-size limit per chunk/job (KB, MB, GB, ...); set to '0' to skip chunking all together (1 job total).",
        )
    if (not only) or ("--fast-forward" in only):
        parser.add_argument(
            "--fast-forward",
            "-f",
            default=False,
            action="store_true",
            help="If *STAGING* files already exist, pick up where it left off"
            " -- useful for condor restarts and tweaking controls. "
            "This will look for traverse.unique, traverse.sorted, and traverse.raw files.",
        )

    return parser
