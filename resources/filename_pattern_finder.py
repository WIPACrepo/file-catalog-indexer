"""Script for finding filename patterns."""

import argparse
import logging
import os
import re
import subprocess
from typing import Dict

import yaml

try:
    from typing import TypedDict
except:  # noqa: E722 # pylint: disable=W0702
    TypedDict = Dict

I3RP = "i3-redacted-paths"
NON_I3RP = f"non-{I3RP}"


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
    allowed_substrs = [
        "i3.bz2",
        "i3",
        "level2",
        "Level2",
        "level3",
        "Level3",
        "level4",
        "Level4",
    ]
    assert len(allowed_substrs) < 32  # there are only 32 non-printable chars

    def _temp_replace(line: str) -> str:
        for i, char in enumerate(allowed_substrs):
            line = re.sub(char, chr(i), line)
        return line

    def _replace_back(line: str) -> str:
        for i, char in enumerate(allowed_substrs):
            line = re.sub(chr(i), char, line)
        return line

    with open(f"{NON_I3RP}.raw", "w") as nonf, open(f"{I3RP}.raw", "w") as i3f:
        with open(fpath, "r") as f:
            for line in f.readlines():
                # weird file, probably some kind of backup file
                if "#" in line:
                    logging.warning(f'"#" in filepath: {line.strip()}')
                else:
                    line = _temp_replace(line)
                    line = re.sub(r"\d+", "#", line)  # replace strings of digits w/ '#'
                    line = _replace_back(line)
                    # .i3 file
                    if ".i3" in line:
                        i3f.write(line)
                    # non-i3 file
                    else:
                        nonf.write(line)

    subprocess.check_call(f"sort {NON_I3RP}.raw > {NON_I3RP}", shell=True)
    os.remove(f"{NON_I3RP}.raw")
    subprocess.check_call(f"sort {I3RP}.raw > {I3RP}", shell=True)
    os.remove(f"{I3RP}.raw")


def summarize(fname: str) -> None:
    """Create a YAML summary with filename patterns."""

    class _PatternSummary(TypedDict):
        count: int
        dirs: Dict[str, int]

    summary: Dict[str, _PatternSummary] = {}

    with open(fname, "r") as f:
        for line in f:
            match = re.match(r"(?P<dpath>.+)/(?P<fname_pattern>[^/]+)$", line.strip())
            if match:
                # get substrings
                fname_pattern = match.groupdict()["fname_pattern"]
                dpath = match.groupdict()["dpath"]
                # allocate
                if fname_pattern not in summary:
                    summary[fname_pattern] = {"dirs": {}, "count": 0}
                if dpath not in summary[fname_pattern]["dirs"]:
                    summary[fname_pattern]["dirs"][dpath] = 0
                # increment
                summary[fname_pattern]["dirs"][dpath] += 1
                summary[fname_pattern]["count"] += 1
            else:
                logging.info(f"no match: '{line.strip()}'")

    with open(f"{fname}-summary.yaml", "w") as f:
        yaml.dump(
            dict(sorted(summary.items(), key=lambda ps: ps[1]["count"], reverse=True)),
            f,
            sort_keys=False,
        )


def main() -> None:
    """Find patterns."""
    parser = argparse.ArgumentParser(
        description="Find patterns in the list of filepaths provided",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--file", help="file that contains a filepath on each line", type=get_full_path,
    )
    args = parser.parse_args()

    if I3RP in os.listdir(".") and NON_I3RP in os.listdir("."):
        logging.info("Using existing redacted-paths.txt")
    elif not args.file:
        logging.critical(f"must have './{I3RP}' and './{NON_I3RP}'; OR use --file")
        raise RuntimeError(f"must have './{I3RP}' and './{NON_I3RP}'; OR use --file")
    else:
        logging.info(f"Parsing {args.file}...")
        redact(args.file)

    for fname in [I3RP, NON_I3RP]:
        summarize(fname)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
