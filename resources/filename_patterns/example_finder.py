"""Get an example of each regex pattern for /data/sim/."""


import re

with open("data-sim-fpatterns.regex") as f:
    for line in f:
        pat = re.compile(".*/" + line.strip())
        print(pat)
        with open("/data/user/eevans/data-sim-2020-12-03T14:11:32") as all_f:
            for all_line in all_f:
                # print('.',end='')
                if re.match(pat, all_line.strip()):
                    print(all_line.strip())
                    with open("examples.txt", "a") as out_f:
                        print(all_line.strip(), file=out_f)
                    break
