"""Print all the file catalog entries with a given filepath root."""


import argparse
import itertools
import json
import logging
import os
from typing import Dict, List, Optional, TypedDict, Union

import coloredlogs  # type: ignore[import]

# local imports
from rest_tools.client import RestClient  # type: ignore[import]

BATCH_SIZE = 10000


class _Match(TypedDict):
    logical_name: str
    uuid: str


def find_all_paths(
    rc: RestClient,
    root: str,
    outdir: str,
    start: int = 0,
    processing_level: Optional[str] = None,
) -> int:
    """Find FC files with `'logical_name'`s starting w/ `root`."""
    prefix = root.replace("/", "-")  # ex: "/data/exp/" -> "-data-exp-"
    if processing_level is not None:
        prefix += f".{processing_level}"
    paths_outfile = os.path.join(outdir, prefix + ".paths")
    infos_outfile = os.path.join(outdir, prefix + ".infos")

    with open(paths_outfile, "a+") as paths_f, open(infos_outfile, "a+") as infos_f:
        total = 0
        for i in itertools.count(start):
            logging.info(f"i={i} ({i*BATCH_SIZE})")
            matches: List[_Match] = []

            # request
            body: Dict[str, Union[int, str]] = {"start": i * BATCH_SIZE}
            if processing_level is not None:
                body["query"] = json.dumps({"processing_level": processing_level})
            files = rc.request_seq("GET", "/api/files", body)["files"]

            if not files:
                raise Exception("no files in response")

            # find matches
            for fcfile in files:
                if fcfile["logical_name"].startswith(root):
                    matches.append(
                        {"logical_name": fcfile["logical_name"], "uuid": fcfile["uuid"]}
                    )
            logging.info(f"new files found: {len(matches)}")

            if not matches:
                continue

            # increment total
            total += len(matches)
            logging.info(
                f"total files: {total} ({(total/((i+1)*BATCH_SIZE))*100:.2f}%)"
            )

            # write matches to outfiles
            for m in matches:  # pylint: disable=C0103
                # *.paths
                logging.info(f"Appending to {paths_f.name}...")
                print(m["logical_name"], file=paths_f)
                # *.infos
                logging.info(f"Appending to {infos_f.name}...")
                print(f'{m["logical_name"]} - {m["uuid"]} - i={i}', file=infos_f)

    return total


if __name__ == "__main__":
    coloredlogs.install(level=logging.INFO)

    # Args
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("--token", required=True, help="file catalog token")
    parser.add_argument(
        "--outdir", required=True, help="dest to write outfiles with results"
    )
    parser.add_argument("--start", type=int, default=0, help="starting batch index")
    parser.add_argument("--root", default="/mnt/", help="filepath root to search for")
    parser.add_argument(
        "--processing-level",
        default=None,
        help="optional processing-level to filter by",
    )
    args = parser.parse_args()

    # Go
    TOTAL = find_all_paths(
        RestClient(
            "https://file-catalog.icecube.wisc.edu/", token=args.token, retries=100,
        ),
        args.root,
        args.outfile,
        args.start,
        args.processing_level,
    )
    logging.info(f"Total paths starting with {args.root}: {TOTAL}")
