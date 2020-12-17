"""Given a filename pattern, produce the regex.

Use with: grep -P `python filename_pattern_regexer.py <string>` <file>
"""


import sys

sys.path.append(".")
from filename_pattern_finder import (  # isort:skip  # noqa # pylint: disable=C0413
    I3_EXT_TOKEN,
    I3_EXTENSIONS,
)


#
# First-stage tokenization

string = sys.argv[1]
string = string.replace("YYYY", r"\d\d\d\d")
string = string.replace("#", r"\d+")
string = string.replace("^", r"\d+")

I3_EXT_REGEX = "(" + "|".join(x.replace(".", r"\.") for x in I3_EXTENSIONS) + ")"
string = string.replace(I3_EXT_TOKEN, I3_EXT_REGEX)


#
# Second-stage tokenization

string = string.replace("EFFNUM?", r"((\.|_)eff\d+)?")

string = string.replace("DATNUM", r"DAT\d+")
string = string.replace("VNUM", r"V\d+")
string = string.replace("STEPNUM", r"(S|s)tep\d+")
string = string.replace("EFFNUM", r"(\.|_)eff\d+")
string = string.replace("PASSNUM", r"(P|p)ass\d+")
string = string.replace("DENUMPNUM", r"DE\d+P\d+")


#
# Print

print(string)
