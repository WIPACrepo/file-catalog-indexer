"""Script for finding filename patterns."""

import argparse
import logging
import os
import pprint
import re
import subprocess
from datetime import datetime
from typing import Dict, List

import coloredlogs  # type: ignore[import]
import yaml

try:
    from typing import TypedDict
except:  # noqa: E722 # pylint: disable=W0702
    TypedDict = Dict


coloredlogs.install(level="DEBUG")


# CONSTANTS ----------------------------------------------------------------------------

I3_EXTENSIONS = [".i3", ".i3.gz", ".i3.bz2", ".i3.zst"]  # excl: .log, .err, .out, .json
logging.info(f"Using i3 extensions: {I3_EXTENSIONS}")
I3_EXT_TOKEN = ".I3EXT$"
EFF_NUM_OPT = "EFFNUM?"
EFF_NUM_REGEX = r"(\.|_)eff#"

I3_PATTERNS = "i3-patterns"
NON_I3_PATTERNS = f"non-{I3_PATTERNS}"

MIN_YEAR, MAX_YEAR = 2000, datetime.now().year + 5
logging.info(f"Using year range {MIN_YEAR}-{MAX_YEAR}")
YEARS = list(range(MIN_YEAR, MAX_YEAR))

TOKEN_SUMMARY_DIR = "token-summaries"
IC_SUMMARY_YAML = os.path.join(TOKEN_SUMMARY_DIR, "ICs.summary.yaml")
DIR_YEARS_SUMMARY_YAML = os.path.join(TOKEN_SUMMARY_DIR, "dir-years.summary.yaml")
FNAME_YEARS_SUMMARY_YAML = os.path.join(TOKEN_SUMMARY_DIR, "file-years.summary.yaml")


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

    # summaries
    dir_years: Dict[int, int] = {k: 0 for k in YEARS}
    fname_years: Dict[int, int] = {k: 0 for k in YEARS}
    ics: Dict[str, int] = {}

    # Write redactions
    with open(f"{NON_I3_PATTERNS}.tmp", "w") as nonf, open(
        f"{I3_PATTERNS}.tmp", "w"
    ) as i3f:
        with open(fpath, "r") as f:
            for line in f:
                red_line = line.strip()
                # weird file, probably some kind of backup file
                if "#" in red_line:
                    logging.warning(f'"#" in filepath: {red_line}')
                # another weird file
                elif "^" in red_line:
                    logging.warning(f'"^" in filepath: {red_line}')
                # a normal file
                else:
                    red_line = _replace_special_digit_substrs(red_line)
                    # year-like substrings
                    for i in YEARS:
                        if f"{i}" in red_line:
                            red_line = red_line.replace(str(i), "YYYY")
                            if "/YYYY/" in red_line:
                                dir_years[i] += 1
                            if re.match(r".*YYYY[^/]*$", red_line):
                                fname_years[i] += 1
                    # IC substrings
                    if "IC" in red_line:
                        for match in re.finditer(r"(IC|ic)(-)?\d+(-\d+)?", red_line):
                            ic_str = match.group(0)
                            try:
                                ics[ic_str] += 1
                            except KeyError:
                                ics[ic_str] = 1
                        for ic in ["ic", "IC"]:  # pylint: disable=C0103
                            red_line = re.sub(rf"{ic}\d+-\d+", f"{ic}^-^", red_line)
                            red_line = re.sub(rf"{ic}\d+", f"{ic}^", red_line)
                            red_line = re.sub(rf"{ic}-\d+-\d+", f"{ic}-^-^", red_line)
                            red_line = re.sub(rf"{ic}-\d+", f"{ic}-^", red_line)
                    # strings of digits -> '#'
                    red_line = re.sub(r"\d+", "#", red_line)
                    red_line = _replace_back_special_digit_substrs(red_line)
                    # .i3 file
                    if ".i3" in red_line:
                        # regex-ify i3 extensions
                        for ext in I3_EXTENSIONS:
                            if red_line.endswith(ext):
                                red_line = red_line.replace(ext, I3_EXT_TOKEN)
                        print(red_line, file=i3f)
                    # non-i3 file
                    else:
                        print(red_line, file=nonf)

    # Sort & Cleanup
    for summary_fname in [NON_I3_PATTERNS, I3_PATTERNS]:
        subprocess.check_call(f"sort {summary_fname}.tmp > {summary_fname}", shell=True)
        os.remove(f"{summary_fname}.tmp")

    # Make Token Summaries
    os.mkdir(TOKEN_SUMMARY_DIR)

    # Dump summaries
    for yaml_fname, summary in [
        (IC_SUMMARY_YAML, sorted(ics.items(), key=lambda ic: ic[1], reverse=True)),
        (FNAME_YEARS_SUMMARY_YAML, dir_years),
        (DIR_YEARS_SUMMARY_YAML, fname_years),
    ]:
        with open(yaml_fname, "w") as f:
            logging.debug(f"Dumping to {yaml_fname}...")
            yaml.dump(dict(summary), f, sort_keys=(yaml_fname != IC_SUMMARY_YAML))  # type: ignore[call-overload]
        logging.debug(f"Dumped {yaml_fname}.")

    logging.info(f"Redacted {fpath}.")


class _FilenamePatternSummary(TypedDict):
    count: int
    dirs: Dict[str, int]


def _coalesce_effnum_patterns(
    fpattern_info: Dict[str, _FilenamePatternSummary]
) -> None:
    r"""Coalesce patterns with r"(\.|_)eff#" w/ patterns that are similar."""
    eff_nums: List[str] = []
    for pattern in fpattern_info.keys():
        match = re.match(rf".*{EFF_NUM_REGEX}.*", pattern)
        if not match:
            continue
        eff_nums.append(pattern)

    def _coallesce_dir_counts(
        one: Dict[str, int], two: Dict[str, int]
    ) -> Dict[str, int]:
        new = {k: v for k, v in one.items()}  # pylint: disable=R1721
        for dir_ct in two.keys():
            try:
                new[dir_ct] += two[dir_ct]
            except KeyError:
                new[dir_ct] = two[dir_ct]
        return new

    for pattern in fpattern_info.keys():
        for eff in eff_nums:
            match = re.match(r"(?P<before>.*)(\.|_)eff#(?P<after>.*)", eff)
            if match.groupdict()["before"] + match.groupdict()["after"] in pattern:  # type: ignore[union-attr]
                try:
                    eff_opt = f'{match.groupdict()["before"]}{EFF_NUM_OPT}{match.groupdict()["after"]}'  # type: ignore[union-attr]
                    fpattern_info[eff_opt]["count"] = (
                        fpattern_info[eff]["count"] + fpattern_info[pattern]["count"]
                    )
                    fpattern_info[eff_opt]["dirs"] = _coallesce_dir_counts(
                        fpattern_info[eff]["dirs"], fpattern_info[pattern]["dirs"]
                    )
                    del fpattern_info[eff]
                    del fpattern_info[pattern]
                except KeyError:
                    pprint.pprint(eff_nums)
                    raise Exception(
                        f"Pattern matches multiple eff#'s: {pattern}; newest eff#: {eff}"
                    )


def summarize(fname: str) -> None:
    """Create a YAML summary with filename patterns."""
    logging.info(f"Summarizing {fname}...")
    dir_ = f"{fname}-summaries"
    os.mkdir(dir_)

    fpattern_info: Dict[str, _FilenamePatternSummary] = {}

    with open(fname, "r") as f:
        logging.debug(f"Parsing {fname}...")
        for line in f:
            match = re.match(r"(?P<dpath>.+)/(?P<fname_pattern>[^/]+)$", line.strip())
            if match:
                # get substrings
                fname_pattern = match.groupdict()["fname_pattern"]
                dpath = match.groupdict()["dpath"]
                # allocate
                if fname_pattern not in fpattern_info:
                    fpattern_info[fname_pattern] = {"dirs": {}, "count": 0}
                if dpath not in fpattern_info[fname_pattern]["dirs"]:
                    fpattern_info[fname_pattern]["dirs"][dpath] = 0
                # increment
                fpattern_info[fname_pattern]["dirs"][dpath] += 1
                fpattern_info[fname_pattern]["count"] += 1
            else:
                logging.debug(f"no match: '{line.strip()}'")

    # Coalesce r"(\.|_)eff#"
    _coalesce_effnum_patterns(fpattern_info)

    # Prep for yamls
    dir_patterns = sorted(
        fpattern_info.items(), key=lambda ps: ps[1]["count"], reverse=True
    )
    counts = {sort_sum[0]: sort_sum[1]["count"] for sort_sum in dir_patterns}

    # Dump summaries
    for yaml_fname, summary in [
        (os.path.join(dir_, f"{fname}.dir-patterns.yaml"), dir_patterns),
        (os.path.join(dir_, f"{fname}.counts.yaml"), counts),
    ]:
        with open(yaml_fname + ".tmp", "w") as f:
            logging.debug(f"Dumping to {yaml_fname}.tmp...")
            # dump in descending order of frequency
            yaml.dump(dict(summary), f, sort_keys=False)  # type: ignore[call-overload]
        os.rename(yaml_fname + ".tmp", yaml_fname)
        logging.debug(f"Dumped {yaml_fname}.")

    logging.info(f"Summarized {fname}.")


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

    if I3_PATTERNS in os.listdir(".") and NON_I3_PATTERNS in os.listdir("."):
        logging.info(f"Using existing './{I3_PATTERNS}' and './{NON_I3_PATTERNS}'")
    elif not args.file:
        logging.critical(
            f"must have './{I3_PATTERNS}' and './{NON_I3_PATTERNS}'; OR use --file"
        )
        raise RuntimeError(
            f"must have './{I3_PATTERNS}' and './{NON_I3_PATTERNS}'; OR use --file"
        )
    else:
        redact(args.file)

    for fname in [I3_PATTERNS, NON_I3_PATTERNS]:
        summarize(fname)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
