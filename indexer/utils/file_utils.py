"""Utilities for file processing."""

import logging
import os
import stat
import string
from typing import List, Optional


def commonpath(paths: List[str]) -> str:
    """Wrap `os.path.commonpath()`."""
    if len(set(paths)) == 1:  # small optimization
        return paths[0]
    try:
        return str(os.path.commonpath(paths))
    except ValueError as e:
        raise ValueError(f'{e}: {", ".join(p for p in paths)}') from e


def is_processable_path(path: str) -> bool:
    """Return `True` if `path` is processable.

    AKA, not a symbolic link, a socket, a FIFO, a device, nor char device.

    Raises:
        FileNotFoundError - if `path` does not exist
    """
    mode = os.lstat(path).st_mode
    ok = not (
        stat.S_ISLNK(mode)
        or stat.S_ISSOCK(mode)
        or stat.S_ISFIFO(mode)
        or stat.S_ISBLK(mode)
        or stat.S_ISCHR(mode)
    )
    if not ok:
        logging.warning(
            f"File is not processable "
            f"(either a symbolic link, socket, FIFO, device, or char device): '{path}'"
        )
    return ok


def get_subpaths(filepath: str) -> List[str]:
    """Get all nested filepaths at directory, `filepath`.

    Don't add symbolic links.
    """

    def is_a_symlink(dir_entry: os.DirEntry) -> bool:  # type: ignore[type-arg]
        is_sym = dir_entry.is_symlink()
        if is_sym:
            logging.warning(
                f"Skipping nested file -- not processable (symbolic link): '{dir_entry}'"
            )
        return is_sym

    return [
        dir_entry.path
        for dir_entry in os.scandir(filepath)
        if not is_a_symlink(dir_entry)
    ]


def sorted_unique_filepaths(
    file_of_filepaths: Optional[str] = None,
    list_of_filepaths: Optional[List[str]] = None,
    abspaths: bool = False,
) -> List[str]:
    """Return an aggregated, sorted, and set-unique list of filepaths.

    Read in lines from the `file_of_filepaths` file, and/or aggregate with those
    in `list_of_filepaths` list. Do not check if filepaths exist.

    Keyword Arguments:
        file_of_filepaths -- a file with a filepath on each line
        list_of_filepaths -- a list of filepaths
        abspaths -- call `os.path.abspath()` on each filepath

    Returns:
        List[str] -- all unique filepaths
    """

    def convert_to_good_string(b_string: bytes) -> Optional[str]:
        # strip trailing new-line char
        if b_string[-1] == ord("\n"):
            b_string = b_string[:-1]
        # ASCII parse
        for b_char in b_string:
            if not (ord(" ") <= b_char <= ord("~")):  # pylint: disable=C0325
                logging.info(
                    f"Invalid filename, {b_string!r}, has special character(s)."
                )
                return None
        # Decode UTF-8
        try:
            path = b_string.decode("utf-8", "strict").rstrip()
        except UnicodeDecodeError as e:
            logging.info(f"Invalid filename, {b_string!r}, {e.__class__.__name__}.")
            return None
        # Non-printable chars
        if not set(path).issubset(string.printable):
            logging.info(f"Invalid filename, {path}, has non-printable character(s).")
            return None
        # all good
        return path

    filepaths = []
    if list_of_filepaths:
        filepaths.extend(list_of_filepaths)
    if file_of_filepaths:
        with open(file_of_filepaths, "rb") as bin_file:
            for bin_line in bin_file:
                path = convert_to_good_string(bin_line)
                if path:
                    filepaths.append(path)

    if abspaths:
        filepaths = [os.path.abspath(p) for p in filepaths]
    filepaths = [f for f in sorted(set(filepaths)) if f]
    return filepaths
