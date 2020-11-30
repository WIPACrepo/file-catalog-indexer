"""Script for finding filename patterns."""

import argparse
import logging
import os
import re
import subprocess
from typing import Dict, Set

import yaml

try:
    from typing import TypedDict
except:  # noqa: E722 # pylint: disable=W0702
    TypedDict = Dict


def get_full_path(path: str) -> str:
    """Check that the path exists and return the full path."""
    if not path:
        return path

    full_path = os.path.abspath(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(full_path)

    return full_path


def redact(fpath: str) -> str:
    """Write out basic patterns; return name of out-file."""
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

    return "redacted.sort"

    # subprocess.check_call("uniq redacted.sort > redacted.txt", shell=True)
    # os.remove("redacted.sort")


def summarize(fname: str) -> None:
    """Create a YAML summary with filename patterns."""

    class _PatternSummary(TypedDict):
        count: int
        dirs: Set[str]

    summary: Dict[str, _PatternSummary] = {}

    with open(fname, "r") as f:
        for line in f:
            match = re.match(r"(?P<dpath>.+)/(?P<fname_pattern>[^/]+)$", line.strip())
            if match:
                fname_pattern = match.groupdict()["fname_pattern"]
                if fname_pattern not in summary:
                    summary[fname_pattern] = {"dirs": set(), "count": 0}
                summary[fname_pattern]["dirs"].add(match.groupdict()["dpath"])
                summary[fname_pattern]["count"] += 1
            else:
                logging.info(f"no match: '{line.strip()}'")

    with open("summary.yaml") as f:
        yaml.dump(
            dict(sorted(summary.items(), key=lambda ps: ps[1]["count"], reverse=True)),
            f,
        )


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

    fname = redact(args.file)

    summarize(fname)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
