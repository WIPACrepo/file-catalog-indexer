"""Script for finding filename patterns."""

import argparse
import logging
import os
import re
import subprocess
from datetime import datetime
from typing import Dict

import yaml

try:
    from typing import TypedDict
except:  # noqa: E722 # pylint: disable=W0702
    TypedDict = Dict

I3RP = "i3-redacted-paths"
NON_I3RP = f"non-{I3RP}"
MIN_YEAR, MAX_YEAR = 2000, datetime.now().year + 5
logging.info(f"Using year range {MIN_YEAR}-{MAX_YEAR}")
YEARS = list(range(MIN_YEAR, MAX_YEAR))

IC_SUMMARY_YAML = "ic-replacement-summary.yaml"
YEARS_SUMMARY_YAML = "year-replacement-summary.yaml"


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
    logging.info(f"Redacting {fpath}...")

    allowed_substrs = [
        "i3.bz2",
        "i3",
        "level1",
        "Level1",
        "level2",
        "Level2",
        "level3",
        "Level3",
        "level4",
        "Level4",
        "level5",
        "Level5",
        "SPICE1",
        "SPICE-1",
        "SPASE-2",
    ]
    assert len(allowed_substrs) < 32  # there are only 32 non-printable chars

    def _temp_replace(line: str) -> str:
        for i, substr in enumerate(allowed_substrs):
            line = line.replace(substr, chr(i))
        return line

    def _replace_temps_back(line: str) -> str:
        for i, substr in enumerate(allowed_substrs):
            line = line.replace(chr(i), substr)
        return line

    years_summary: Dict[int, int] = {k: 0 for k in YEARS}
    ics_summary: Dict[str, int] = {}

    with open(f"{NON_I3RP}.raw", "w") as nonf, open(f"{I3RP}.raw", "w") as i3f:
        with open(fpath, "r") as f:
            for line in f.readlines():
                # weird file, probably some kind of backup file
                if "#" in line:
                    logging.warning(f'"#" in filepath: {line.strip()}')
                else:
                    # special digit-substrings
                    line = _temp_replace(line)
                    for i in YEARS:
                        if f"/{i}/" in line:
                            line = line.replace(f"/{i}/", "/YYYY/")
                            years_summary[i] += 1
                    if "IC" in line:
                        for match in re.finditer(r"IC(-)?\d+(-\d+)?", line):
                            ic_str = match.group(0)
                            try:
                                ics_summary[ic_str] += 1
                            except KeyError:
                                ics_summary[ic_str] = 1
                        line = re.sub(r"IC\d+-\d+", "IC^-^", line)
                        line = re.sub(r"IC\d+", "IC^", line)
                        line = re.sub(r"IC-\d+-\d+", "IC-^-^", line)
                        line = re.sub(r"IC-\d+", "IC-^", line)
                    # strings of digits -> '#'
                    line = re.sub(r"\d+", "#", line)
                    line = _replace_temps_back(line)
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

    with open(IC_SUMMARY_YAML, "w") as f:
        logging.info(f"Dumping to {IC_SUMMARY_YAML}...")
        yaml.dump(  # dump in descending order of frequency
            dict(sorted(ics_summary.items(), key=lambda ic: ic[1], reverse=True)),
            f,
            sort_keys=False,
        )
    with open(YEARS_SUMMARY_YAML, "w") as f:
        logging.info(f"Dumping to {YEARS_SUMMARY_YAML}...")
        yaml.dump(years_summary, f)

    logging.info(f"Redacted {fpath}: {IC_SUMMARY_YAML} & {YEARS_SUMMARY_YAML}")


def summarize(fname: str) -> None:
    """Create a YAML summary with filename patterns."""
    logging.info(f"Summarizing {fname}...")

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
                logging.debug(f"no match: '{line.strip()}'")

    summary_yaml: str = f"{fname}-summary.yaml"
    with open(summary_yaml, "w") as f:
        logging.info(f"Dumping to {summary_yaml}...")
        yaml.dump(  # dump in descending order of frequency
            dict(sorted(summary.items(), key=lambda ps: ps[1]["count"], reverse=True)),
            f,
            sort_keys=False,
        )

    logging.info(f"Summarized {fname}: {summary_yaml}")


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
        redact(args.file)

    for fname in [I3RP, NON_I3RP]:
        summarize(fname)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
