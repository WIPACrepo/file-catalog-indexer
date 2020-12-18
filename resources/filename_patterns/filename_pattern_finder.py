"""Script for finding filename patterns."""

import argparse
import logging
import os
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
I3_EXT_TOKEN = ".I3EXT"

I3_PATTERNS = "stage-1-i3-patterns"
NON_I3_PATTERNS = "stage-1-non-i3-patterns"

MIN_YEAR, MAX_YEAR = 2000, datetime.now().year + 5
logging.info(f"Using year range {MIN_YEAR}-{MAX_YEAR}")
YEARS = list(range(MIN_YEAR, MAX_YEAR))

TOKEN_SUMMARY_DIR = "stage-1-token-summaries"
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


def stage_1_redact(fpath: str) -> None:
    """Write out basic patterns."""
    logging.info(f"Stage 1: Redacting {fpath}...")

    allowed_substrs = [
        "i3.bz2",
        "i3",
        "level1",
        "Level1",
        "L1",
        "level2",
        "Level2",
        "L2",
        "level3",
        "Level3",
        "L3",
        "level4",
        "Level4",
        "L4",
        "level5",
        "Level5",
        "L5",
        "SPICE1",
        "SPICE-1",
        "SPASE-2",
        "Gen2",
        "KYG1",
        "DE1P",
        "_os100",  # Ex: epos_os100, sibyll-fluka_os100
        "corsika_5comp",
        "corsika_scat_absorption_m7.1",
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
                            if f"/{i}/" in red_line:
                                dir_years[i] += 1
                            if re.match(rf".*{i}[^/]*$", red_line):
                                fname_years[i] += 1
                            red_line = red_line.replace(str(i), "YYYY")
                    # IC substrings
                    if "IC" in red_line or "ic" in red_line:
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
                    for bad_year in ["#YYYY#", "#YYYY", "YYYY#"]:
                        red_line = red_line.replace(bad_year, "#")
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

    logging.info(f"Stage 1: Redacted {fpath}.")


class _FilenamePatternInfo(TypedDict):
    count: int
    dirs: Dict[str, int]


class _SpecialNumStrings(TypedDict):
    find: str
    hash_regex: str
    num_token: str
    normal_regex: str


SPECIAL_NUM_STRINGS: List[_SpecialNumStrings] = [
    {
        "find": "DAT",
        "hash_regex": "DAT#",
        "num_token": "DATNUM",
        "normal_regex": r"DAT\d+",
    },
    {
        "find": "MeV",
        "hash_regex": "#MeV",
        "num_token": r"MEVPRENUM",
        "normal_regex": r"DAT\d+",
    },
    {
        "find": "V",
        "hash_regex": r"(v|V)#",  # ignore MeV#-types
        "num_token": "VICTORNUM",  # phonetic alphabet
        "normal_regex": r"(v|V)\d+",
    },
    {
        "find": "v",
        "hash_regex": r"(v|V)#",  # ignore MeV#-types
        "num_token": "VICTORNUM",  # phonetic alphabet
        "normal_regex": r"(v|V)\d+",
    },
    {
        "find": "tep#",
        "hash_regex": r"(S|s)tep#",
        "num_token": "STEPNUM",
        "normal_regex": r"(S|s)tep\d+",
    },
    {
        "find": "eff#",
        "hash_regex": r"(\.|_)eff#",
        "num_token": "EFFNUM",
        "normal_regex": r"(\.|_)eff\d+",
    },
    {
        "find": "ass#",
        "hash_regex": r"(P|p)ass#",
        "num_token": "PASSNUM",
        "normal_regex": r"(P|p)ass\d+",
    },
    {
        "find": "P#",
        "hash_regex": "P#",
        "num_token": "PAPANUM",  # phonetic alphabet
        "normal_regex": r"P\d+",
    },  # # #
    {
        "find": "sibyll#",
        "hash_regex": r"sibyll#\.#[a-z]?",
        "num_token": "SIBYLNUM",
        "normal_regex": r"sibyll\d\.\d[a-z]?",
    },
    {
        "find": "KYG1_distr_#",
        "hash_regex": "KYG1_distr_#",
        "num_token": "KYG1DISTRNUM",
        "normal_regex": r"KYG1_distr_\d+",
    },
    {
        "find": "ICYYYY",
        "hash_regex": r"ICYYYY",
        "num_token": "INDIACHARLIEYANKEEFOURNUM",  # phonetic alphabet
        "normal_regex": r"IC\d\d\d\d",
    },
    {
        "find": "YYYY",
        "hash_regex": r"IC\^\.YYYY",
        "num_token": "INDIACHARLIENUMYANKEEFOURNUM",  # phonetic alphabet
        "normal_regex": r"IC\d+\.\d\d\d\d",
    },
]
# must use good 'num_token's
for s in SPECIAL_NUM_STRINGS:
    assert len(s["num_token"]) > 4 and s["num_token"].endswith("NUM")
    for s2 in SPECIAL_NUM_STRINGS:
        assert (
            s["num_token"] == s2["num_token"]
            and s["hash_regex"] == s2["hash_regex"]
            and s["normal_regex"] == s2["normal_regex"]
        ) or s["num_token"] not in s2["num_token"]

SPECIAL_SUFFIXES = [
    "as.flasher",
    "clsim-",
    "err_s",
    "only_muons",
    "base",
    "untriggered",
]


def _special_num_strings(fnpat_infos: Dict[str, _FilenamePatternInfo]) -> None:
    for special_num_string in SPECIAL_NUM_STRINGS:
        for fnpat in list(fnpat_infos.keys()):  # collection changes size during iter'n
            if special_num_string["find"] in fnpat:
                new_fnpat = re.sub(
                    special_num_string["hash_regex"],
                    special_num_string["num_token"],
                    fnpat,
                )
                fnpat_infos[new_fnpat] = fnpat_infos.pop(fnpat)


def _special_suffixes(fnpat_infos: Dict[str, _FilenamePatternInfo]) -> None:
    while True:  # repeat for nested suffixes
        made_changes = False
        for suffix in SPECIAL_SUFFIXES:
            for fnpat in list(
                fnpat_infos.keys()
            ):  # collection changes size during iter'n
                if suffix not in fnpat:
                    continue
                match = re.match(rf"(?P<before>.*)({suffix}.*)DOTI3EXT$", fnpat)
                if not match:
                    continue
                made_changes = True
                red_fnpat = f"{match.groupdict()['before']}SUFFIXDOTI3EXT"
                try:  # assume red_fnpat has already been added, so increment counts
                    fnpat_infos[red_fnpat]["count"] += fnpat_infos[fnpat]["count"]
                    for dir_, count in fnpat_infos[fnpat]["dirs"].items():
                        try:  # assume dir as already been added
                            fnpat_infos[red_fnpat]["dirs"][dir_] += count
                        except KeyError:  # new dir
                            fnpat_infos[red_fnpat]["dirs"][dir_] = count
                    del fnpat_infos[fnpat]
                    logging.debug(f"Coalesced Suffix: {fnpat} -> {red_fnpat}")
                except KeyError:  # red_fnpat has NOT already been added
                    fnpat_infos[red_fnpat] = fnpat_infos.pop(fnpat)
                    logging.debug(f"New Suffix: {fnpat} -> {red_fnpat}")
        if not made_changes:
            return


def stage_2_summarize(fname: str) -> None:
    """Create a YAML summary with filename patterns."""
    logging.info(f"Stage 2: Summarizing {fname}...")
    prefix = f"stage-2-{fname.split('stage-1-')[1]}"
    dir_ = f"{prefix}-summaries"
    os.mkdir(dir_)

    fnpat_infos: Dict[str, _FilenamePatternInfo] = {}

    with open(fname, "r") as f:
        logging.debug(f"Parsing {fname}...")
        for line in f:
            match = re.match(r"(?P<dpath>.+)/(?P<fnpat>[^/]+)$", line.strip())
            if match:
                # get substrings
                fnpat = match.groupdict()["fnpat"]
                dpath = match.groupdict()["dpath"]
                # allocate
                if fnpat not in fnpat_infos:
                    fnpat_infos[fnpat] = {"dirs": {}, "count": 0}
                if dpath not in fnpat_infos[fnpat]["dirs"]:
                    fnpat_infos[fnpat]["dirs"][dpath] = 0
                # increment
                fnpat_infos[fnpat]["dirs"][dpath] += 1
                fnpat_infos[fnpat]["count"] += 1
            else:
                logging.debug(f"no match: '{line.strip()}'")

    # special-string tokenization
    _special_num_strings(fnpat_infos)
    _special_suffixes(fnpat_infos)

    # Prep for yamls
    dir_patterns = sorted(
        fnpat_infos.items(), key=lambda ps: ps[1]["count"], reverse=True
    )
    counts = {sort_sum[0]: sort_sum[1]["count"] for sort_sum in dir_patterns}

    # Dump summaries
    for yaml_fname, summary in [
        (os.path.join(dir_, f"{prefix}.dir-patterns.yaml"), dir_patterns),
        (os.path.join(dir_, f"{prefix}.counts.yaml"), counts),
    ]:
        with open(yaml_fname + ".tmp", "w") as f:
            logging.debug(f"Dumping to {yaml_fname}.tmp...")
            # dump in descending order of frequency
            yaml.dump(dict(summary), f, sort_keys=False)  # type: ignore[call-overload]
        os.rename(yaml_fname + ".tmp", yaml_fname)
        logging.debug(f"Dumped {yaml_fname}.")

    logging.info(f"Stage 2: Summarized {fname}.")


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
        stage_1_redact(args.file)

    for fname in [I3_PATTERNS, NON_I3_PATTERNS]:
        stage_2_summarize(fname)

    logging.info("Done.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main()
