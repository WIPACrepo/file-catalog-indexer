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
            " **Ignored if also using --traverse-file**",
            type=get_full_path,
        )
    if (not only) or ("--previous-traverse" in only):
        parser.add_argument(
            "--previous-traverse",
            dest="previous_traverse",
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
            " **Ignored if also using --traverse-file**",
        )
    if (not only) or ("--traverse-file" in only):
        parser.add_argument(
            "--traverse-file",
            dest="traverse_file",
            type=get_full_path,
            default=None,
            help="bypass traversing and use this file instead;"
            " useful for tweaking other controls."
            " **Overrides other arguments, see those for details.**",
        )
    if (not only) or ("--chunk-size" in only):
        parser.add_argument(
            "--chunk-size",
            dest="chunk_size",
            type=_parse_to_bytes,
            default=0,
            help="aggregate file-size limit per chunk/job (bytes, KB, MB, ...), by default, one chunk is made.",
        )

    return parser
