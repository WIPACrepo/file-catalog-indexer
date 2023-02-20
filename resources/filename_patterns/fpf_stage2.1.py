"""Group (stage 2) filename patterns by endings."""

# TODO: remove "mypy: ignore-errors" this file is ever updated
# mypy: ignore-errors

import logging
import re
import sys
from typing import Dict, List, TypedDict

import yaml

raise Exception(
    "Deprecated since introducing num-sequences in filename_pattern_finder.py"
)

yaml_file = sys.argv[1]
if ".counts.yaml" not in yaml_file:
    raise RuntimeError("Enter a *.counts.yaml file")
with open(yaml_file, "r") as f:
    fpats_cts = yaml.safe_load(f)


class _HashEndings(TypedDict):
    matches: List[Dict[str, int]]
    count: int


hash_endings: Dict[str, _HashEndings] = {
    num: {"matches": [], "count": 0} for num in ["5+", "4", "3", "2", "1", "0"]
}

for fpat, count in fpats_cts.items():
    rev_fpat = fpat[::-1]  # reverse string
    # 5+
    if re.match(r"(.*)#(_|\.)#(_|\.)#(_|\.)#(_|\.)#", rev_fpat):
        hash_endings["5+"]["matches"].append({fpat: count})
        hash_endings["5+"]["count"] += count
    # 4
    elif re.match(r"(.*)#(_|\.)#(_|\.)#(_|\.)#", rev_fpat):
        hash_endings["4"]["matches"].append({fpat: count})
        hash_endings["4"]["count"] += count
    # 3
    elif re.match(r"(.*)#(_|\.)#(_|\.)#", rev_fpat):
        hash_endings["3"]["matches"].append({fpat: count})
        hash_endings["3"]["count"] += count
    # 2
    elif re.match(r"(.*)#(_|\.)#", rev_fpat):
        hash_endings["2"]["matches"].append({fpat: count})
        hash_endings["2"]["count"] += count
    # 1
    elif re.match(r"(.*)#", rev_fpat):
        hash_endings["1"]["matches"].append({fpat: count})
        hash_endings["1"]["count"] += count
    # 0
    else:
        logging.warning(f"fpat doesn't have '#':{fpat}")
        hash_endings["0"]["matches"].append({fpat: count})
        hash_endings["0"]["count"] += count

prefix = yaml_file.split(".counts.yaml")[0]
prefix = prefix.replace("2", "2.1")
with open(prefix + ".endings.counts.yaml", "w") as f:
    yaml.dump(hash_endings, f)
