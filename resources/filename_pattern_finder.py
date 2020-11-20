"""Script for finding filename patterns."""

import argparse
import logging
import os
import re
import shutil
import subprocess


def get_full_path(path: str) -> str:
    """Check that the path exists and return the full path."""
    if not path:
        return path

    full_path = os.path.abspath(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(full_path)

    return full_path


def redact(fpath: str) -> None:
    """Write out basic patterns."""
    with open("redacted.raw", "w") as bpf:
        with open(fpath, "r") as f:
            for line in f.readlines():
                if "#" in line:
                    logging.warning(f'"#" in filepath: {line.strip()}')
                # replace each string of digits w/ '#'
                redacted = re.sub(r"\d+", "#", line)
                bpf.write(redacted)

    subprocess.check_call("sort redacted.raw > redacted.sort", shell=True)
    os.remove("redacted.raw")
    subprocess.check_call("uniq redacted.sort > redacted.txt", shell=True)
    os.remove("redacted.sort")


def main() -> None:
    """Find patterns."""
    parser = argparse.ArgumentParser(
        description="Find patterns in the list of filepaths provided",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "file", help="file that contains a filepath on each line", type=get_full_path,
    )
    args = parser.parse_args()

    redact(args.file)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
