"""Make the Condor script for path_collector.py."""

import getpass
import logging
import os
import subprocess
import sys
from typing import List

import coloredlogs  # type: ignore[import]

sys.path.append(".")
from common_args import (  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
    get_parser_w_common_args,
)


def make_condor_scratch_dir(traverse_root: str, with_exclusions: bool = False) -> str:
    """Make the condor scratch directory."""
    name = traverse_root.strip("/").replace("/", "-")  # Ex: 'data-exp'
    dir_name = f"path-collection-{name}"
    if with_exclusions:
        dir_name += "-W-EXCLS"

    scratch = os.path.join("/scratch/", getpass.getuser(), dir_name)
    if not os.path.exists(scratch):
        os.makedirs(scratch)

    return scratch


def make_condor_file(  # pylint: disable=R0913
    scratch: str,
    prev_traverse: str,
    traverse_root: str,
    cpus: int,
    memory: str,
    chunk_size: int,
    excluded_paths: List[str],
) -> str:
    """Make the condor file."""
    condorpath = os.path.join(scratch, "condor")
    with open(condorpath, "w") as file:
        # args
        staging_dir = os.path.join("/data/user/", getpass.getuser())
        transfer_input_files = [
            "path_collector.py",
            "traverser.py",
            "common_args.py",
            "../../requirements.txt",
        ]
        # optional args
        previous_arg = f"--previous-traverse {prev_traverse}" if prev_traverse else ""
        exculdes_args = " ".join(excluded_paths) if excluded_paths else ""
        chunk_size_arg = f"--chunk-size {chunk_size}" if chunk_size else ""

        # write
        file.write(
            f"""executable = {os.path.abspath('../indexer_env.sh')}
arguments = python path_collector.py {traverse_root} --staging-dir {staging_dir} --workers {cpus} {previous_arg} --exclude {exculdes_args} {chunk_size_arg}
output = {scratch}/path_collector.out
error = {scratch}/path_collector.err
log = {scratch}/path_collector.log
+FileSystemDomain = "blah"
should_transfer_files = YES
transfer_input_files = {",".join([os.path.abspath(f) for f in transfer_input_files])}
request_cpus = {cpus}
request_memory = {memory}
notification = Error
queue
"""
        )

    return condorpath


def main() -> None:
    """Prep and execute Condor job (to run path_collector.py).

    Make scratch directory and condor file.
    """
    if not os.getcwd().endswith("file-catalog-indexer/resources/path_collector"):
        raise RuntimeError(
            "You must run this script from"
            " `file-catalog-indexer/resources/path_collector`."
            " This script uses relative paths."
        )

    parser = get_parser_w_common_args(
        "Make Condor script for path_collector.py: "
        "recursively find all filepaths in `traverse_root`, "
        "place path_collector.py's output files in /data/user/{user}/, and "
        "Condor log files in /scratch/{user}/path-collection-{traverse_root_w_dashes}/."
    )
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="does everything except submitting the condor job(s)",
    )
    parser.add_argument("--cpus", type=int, help="number of CPUs", default=8)
    parser.add_argument("--memory", help="amount of memory", default="20GB")
    args = parser.parse_args()

    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    # make condor scratch directory
    scratch = make_condor_scratch_dir(args.traverse_root, bool(args.exclude))

    # make condor file
    condorpath = make_condor_file(
        scratch,
        args.previous_traverse,
        args.traverse_root,
        args.cpus,
        args.memory,
        args.chunk_size,
        args.exclude,
    )

    # Execute
    if args.dryrun:
        logging.error(f"Script Aborted: Condor job not submitted ({condorpath}).")
    else:
        cmd = f"condor_submit {condorpath}"
        logging.info(cmd)
        subprocess.check_call(cmd.split(), cwd=scratch)


if __name__ == "__main__":
    coloredlogs.install(level="DEBUG")
    main()