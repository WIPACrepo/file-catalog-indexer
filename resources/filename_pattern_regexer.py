"""Given a filename pattern, produce the regex.

Use with: grep -P `python filename_pattern_regexer.py <string>` <file>
"""


import sys

string = sys.argv[1]
string = string.replace("YYYY", r"\d\d\d\d")
string = string.replace("#", r"\d+")
string = string.replace("^", r"\d+")

print(string)
