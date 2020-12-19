"""Simple script for printing filename-pattern frequencies.

stdout has format, Ex: "<frequency> - <filename-pattern-regex>"
"""

import sys

import matplotlib.pyplot as plt  # type: ignore[import]
import yaml

# get data

with open(sys.argv[1]) as f:
    pat_freq = yaml.load(f, Loader=yaml.FullLoader)


# print

for pat, freq in pat_freq.items():
    print(f"{str(freq):>7s} - {pat}")  # 5931227


# plot

fig, ax = plt.subplots()
ax.plot(pat_freq.keys(), pat_freq.values())

plt.xlabel("Filename Pattern (Indexed)")
plt.ylabel("Frequency")
plt.title(f"Filename Patterns by Frequency {sys.argv[2] if len(sys.argv) > 2 else ''}")
plt.yscale("log", nonpositive="clip")
TICK = 200
plt.xticks([tick if i % TICK == 0 else "" for i, tick in enumerate(pat_freq.keys())])
ax.xaxis.set_ticklabels([i if i % TICK == 0 else "" for i, _ in enumerate(pat_freq)])
plt.show()
