"""Given a filename pattern, produce the regex.

Use with: grep -P `python filename_pattern_regexer.py <string>` <file>
"""


import logging
import sys

sys.path.append(".")
from filename_pattern_finder import (  # isort:skip  # noqa # pylint: disable=C0413
    I3_EXT_TOKEN,
    I3_EXTENSIONS,
    SPECIAL_NUM_STRINGS,
    SPECIAL_SUFFIXES,
)

#
# Prep

string = sys.argv[1]
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
        special_num_string["num_token"], special_num_string["normal_regex"]
    )

SPECIAL_SUFFIXES_REGEX = (
    "(" + "|".join(x.replace(".", r"\.") for x in SPECIAL_SUFFIXES) + ")"
)
string = string.replace("SUFFIX", SPECIAL_SUFFIXES_REGEX)


#
# Print

logging.info(string)
print(string)
