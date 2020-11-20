"""Find the bad path."""
import sys

# finds finished filepaths
finished = []
with open(sys.argv[1], "r") as f:
    for line in f:
        if "Scan finished, directory" in line:
            finished.append(line.split(": ")[1].strip())

# find 'scanning' filepaths
scanning = []
with open(sys.argv[1], "r") as f:
    for line in f:
        if "Scanning directory" in line:
            scanning.append(line.split(": ")[1].split("...")[0])

# find unfinished filepaths
for fpath in scanning:
    if fpath not in finished:
        print(fpath)
