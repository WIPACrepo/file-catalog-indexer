"""Given a filename pattern, produce the regex.

Use with: grep -P `python filename_pattern_regexer.py <string>` <file>
"""


import logging
import re
import sys
from typing import List

import yaml

sys.path.append(".")
from filename_pattern_finder import (  # isort:skip  # noqa # pylint: disable=C0413
    I3_EXT_TOKEN,
    I3_EXTENSIONS,
    SPECIAL_NUM_STRINGS,
    SPECIAL_SUFFIXES,
    NUM_SEQUENCES,
)

#
# Prep

strings: List[str] = []

if sys.argv[1].endswith(".yaml"):
    with open(sys.argv[1], "r") as f:
        in_yaml = yaml.load(f)
    if isinstance(in_yaml, dict):
        strings = list(in_yaml.keys())
    elif isinstance(in_yaml, list):
        strings = in_yaml
    elif isinstance(in_yaml, set):
        strings = list(in_yaml)
    else:
        raise Exception(f"Unsupported yaml type: {type(in_yaml)}")
else:
    strings = [sys.argv[1]]

# Regex-ify!
for string in strings:
    pattern = string.replace(".", r"\.")

    #
    # First-stage tokenization

    pattern = pattern.replace("YYYY", r"\d\d\d\d")
    pattern = pattern.replace("#", r"\d+")
    pattern = pattern.replace("^", r"\d+")

    I3_EXT_REGEX = "(" + "|".join(x.replace(".", r"\.") for x in I3_EXTENSIONS) + ")"
    pattern = pattern.replace(I3_EXT_TOKEN.replace(".", r"\."), I3_EXT_REGEX)

    #
    # Second-stage tokenization

    # special num strings
    for special_num_string in SPECIAL_NUM_STRINGS:
        pattern = pattern.replace(
            special_num_string["token"], f"({special_num_string['normal_regex']})",
        )

    # suffixes
    SPECIAL_SUFFIXES_REGEX = (
        "(" + "|".join(x.replace(".", r"\.") for x in SPECIAL_SUFFIXES) + ").*)"
    )
    pattern = pattern.replace("SUFFIX", SPECIAL_SUFFIXES_REGEX)

    # num sequences
    for num_sequence in NUM_SEQUENCES:
        pattern = pattern.replace(
            num_sequence["token"], f"({num_sequence['normal_regex']})"
        )
    # each string should only have 1 alpha&beta num-seq groups each
    # keep the last one, AKA remove others names
    for group_name in ["?P<alpha>", "?P<beta>"]:
        if pattern.count(group_name) < 2:
            continue
        parts = pattern.split(group_name)
        pattern = "".join(parts[:-1]) + group_name + parts[-1]
        logging.debug(f"Removed duplicate '{group_name}' group names: {pattern}.")

    pattern = pattern + "$"

    #
    # Sanity Check
    re.compile(pattern)

    #
    # Print

    logging.info(pattern)
    print(pattern)
