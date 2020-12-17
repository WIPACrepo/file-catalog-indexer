"""Given a filename pattern, produce the regex.

Use with: grep -P `python filename_pattern_regexer.py <string>` <file>
"""


import sys

sys.path.append(".")
from filename_pattern_finder import (  # isort:skip  # noqa # pylint: disable=C0413
    I3_EXT_TOKEN,
    I3_EXTENSIONS,
    EFF_NUM_OPT,
    EFF_NUM_REGEX,
)


string = sys.argv[1]
string = string.replace("YYYY", r"\d\d\d\d")
string = string.replace("#", r"\d+")
string = string.replace("^", r"\d+")

string = string.replace("DATNUM", r"DAT\d+")
string = string.replace("VNUM", r"V\d+")
string = string.replace("STEPNUM", r"Step\d+")

I3_EXT_REGEX = "(" + "|".join(x.replace(".", r"\.") for x in I3_EXTENSIONS) + ")$"
string = string.replace(I3_EXT_TOKEN, I3_EXT_REGEX)

string = string.replace(EFF_NUM_OPT, f"({EFF_NUM_REGEX})?")

print(string)
