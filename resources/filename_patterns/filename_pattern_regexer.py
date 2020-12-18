"""Given a filename pattern, produce the regex.

Use with: grep -P `python filename_pattern_regexer.py <string>` <file>
"""


import logging
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
    string = string.replace(".", r"\.")

    #
    # First-stage tokenization

    string = string.replace("YYYY", r"\d\d\d\d")
    string = string.replace("#", r"\d+")
    string = string.replace("^", r"\d+")

    I3_EXT_REGEX = "(" + "|".join(x.replace(".", r"\.") for x in I3_EXTENSIONS) + ")"
    string = string.replace(I3_EXT_TOKEN.replace(".", r"\."), I3_EXT_REGEX)

    #
    # Second-stage tokenization

    for special_num_string in SPECIAL_NUM_STRINGS:
        string = string.replace(
            special_num_string["token"],
            f"(?P<{special_num_string['token'].lower()}>{special_num_string['normal_regex']})",
        )

    SPECIAL_SUFFIXES_REGEX = (
        "(" + "|".join(x.replace(".", r"\.") for x in SPECIAL_SUFFIXES) + ").*"
    )
    string = string.replace("SUFFIX", SPECIAL_SUFFIXES_REGEX)

    for num_sequence in NUM_SEQUENCES:
        string = string.replace(
            num_sequence["token"],
            f"(?P<{num_sequence['token'].lower()}>{num_sequence['normal_regex']})",
        )

    #
    # Print

    logging.info(string)
    print(string)
