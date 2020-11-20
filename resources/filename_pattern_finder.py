"""Script for finding filename patterns."""

import argparse
import logging
import os


def get_full_path(path: str) -> str:
    """Check that the path exists and return the full path."""
    if not path:
        return path

    full_path = os.path.abspath(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(full_path)

    return full_path


def main() -> None:
    """Find patterns."""
    parser = argparse.ArgumentParser(
        description="Find patterns in the list of filepaths provided",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "file", help="file that contains a filepath on each line", type=get_full_path,
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
