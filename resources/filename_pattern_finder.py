"""Script for finding filename patterns."""

import argparse
import logging
import os
import re
import subprocess
from datetime import datetime
from typing import Dict

import coloredlogs  # type: ignore[import]
import yaml

try:
    from typing import TypedDict
except:  # noqa: E722 # pylint: disable=W0702
    TypedDict = Dict


coloredlogs.install(level="DEBUG")


# CONSTANTS ----------------------------------------------------------------------------

I3_RED = "i3-paths.redacted"
NON_I3_RED = f"non-{I3_RED}"

MIN_YEAR, MAX_YEAR = 2000, datetime.now().year + 5
logging.info(f"Using year range {MIN_YEAR}-{MAX_YEAR}")
YEARS = list(range(MIN_YEAR, MAX_YEAR))

IC_SUMMARY_YAML = "ic-replacement.summary.yaml"
YEARS_SUMMARY_YAML = "year-replacement.summary.yaml"


# FUNCTIONS ----------------------------------------------------------------------------


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
        "Gen2",
    ]
    assert len(allowed_substrs) < 32  # there are only 32 non-printable chars

    def _replace_special_digit_substrs(fpathline: str) -> str:
        for i, substr in enumerate(allowed_substrs):
            fpathline = fpathline.replace(substr, chr(i))
        return fpathline

    def _replace_back_special_digit_substrs(fpathline: str) -> str:
        for i, substr in enumerate(allowed_substrs):
            fpathline = fpathline.replace(chr(i), substr)
        return fpathline

    years_summary: Dict[int, int] = {k: 0 for k in YEARS}
    ics_summary: Dict[str, int] = {}

    with open(f"{NON_I3_RED}.raw", "w") as nonf, open(f"{I3_RED}.raw", "w") as i3f:
        with open(fpath, "r") as f:
            for line in f:
                redacted_line = line.strip()
                # weird file, probably some kind of backup file
                if "#" in redacted_line:
                    logging.warning(f'"#" in filepath: {redacted_line}')
                # another weird file
                elif "^" in redacted_line:
                    logging.warning(f'"^" in filepath: {redacted_line}')
                # a normal file
                else:
                    redacted_line = _replace_special_digit_substrs(redacted_line)
                    # year-like substrings
                    for i in YEARS:
                        if f"/{i}/" in redacted_line:
                            redacted_line = redacted_line.replace(f"/{i}/", "/YYYY/")
                            years_summary[i] += 1
                    # IC substrings
                    if "IC" in redacted_line:
                        for match in re.finditer(r"IC(-)?\d+(-\d+)?", redacted_line):
                            ic_str = match.group(0)
                            try:
                                ics_summary[ic_str] += 1
                            except KeyError:
                                ics_summary[ic_str] = 1
                        redacted_line = re.sub(r"IC\d+-\d+", "IC^-^", redacted_line)
                        redacted_line = re.sub(r"IC\d+", "IC^", redacted_line)
                        redacted_line = re.sub(r"IC-\d+-\d+", "IC-^-^", redacted_line)
                        redacted_line = re.sub(r"IC-\d+", "IC-^", redacted_line)
                    # strings of digits -> '#'
                    redacted_line = re.sub(r"\d+", "#", redacted_line)
                    redacted_line = _replace_back_special_digit_substrs(redacted_line)
                    # .i3 file
                    if ".i3" in redacted_line:
                        print(f"{redacted_line}", file=i3f)
                    # non-i3 file
                    else:
                        print(f"{redacted_line}", file=nonf)

    subprocess.check_call(f"sort {NON_I3_RED}.raw > {NON_I3_RED}", shell=True)
    os.remove(f"{NON_I3_RED}.raw")
    subprocess.check_call(f"sort {I3_RED}.raw > {I3_RED}", shell=True)
    os.remove(f"{I3_RED}.raw")

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

    class _FilenamePatternSummary(TypedDict):
        count: int
        dirs: Dict[str, int]

    fpattern_summaries: Dict[str, _FilenamePatternSummary] = {}

    with open(fname, "r") as f:
        logging.info(f"Parsing {fname}...")
        for line in f:
            match = re.match(r"(?P<dpath>.+)/(?P<fname_pattern>[^/]+)$", line.strip())
            if match:
                # get substrings
                fname_pattern = match.groupdict()["fname_pattern"]
                dpath = match.groupdict()["dpath"]
                # allocate
                if fname_pattern not in fpattern_summaries:
                    fpattern_summaries[fname_pattern] = {"dirs": {}, "count": 0}
                if dpath not in fpattern_summaries[fname_pattern]["dirs"]:
                    fpattern_summaries[fname_pattern]["dirs"][dpath] = 0
                # increment
                fpattern_summaries[fname_pattern]["dirs"][dpath] += 1
                fpattern_summaries[fname_pattern]["count"] += 1
            else:
                logging.debug(f"no match: '{line.strip()}'")

    summary_yaml: str = f"{fname}.summary.yaml"
    with open(summary_yaml + ".tmp", "w") as f:
        logging.info(f"Dumping to {summary_yaml}.tmp...")
        # dump in descending order of frequency
        yaml.dump(
            dict(
                sorted(
                    fpattern_summaries.items(),
                    key=lambda ps: ps[1]["count"],
                    reverse=True,
                )
            ),
            f,
            sort_keys=False,
        )

    os.rename(summary_yaml + ".tmp", summary_yaml)
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
    coloredlogs.install(level="DEBUG")

    if I3_RED in os.listdir(".") and NON_I3_RED in os.listdir("."):
        logging.info(f"Using existing './{I3_RED}' and './{NON_I3_RED}'")
    elif not args.file:
        logging.critical(f"must have './{I3_RED}' and './{NON_I3_RED}'; OR use --file")
        raise RuntimeError(
            f"must have './{I3_RED}' and './{NON_I3_RED}'; OR use --file"
        )
    else:
        redact(args.file)

    for fname in [I3_RED, NON_I3_RED]:
        summarize(fname)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
