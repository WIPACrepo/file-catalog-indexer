"""Make the Condor script for path_collector.py."""

import getpass
import logging
import os
import subprocess
from typing import List

import coloredlogs  # type: ignore[import]

from resources.path_collector.common_args import (  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
    get_parser_w_common_args,
    get_full_path,
)


def make_condor_scratch_dir(traverse_root: str) -> str:
    """Make the condor scratch directory."""
    name = traverse_root.strip("/").replace("/", "-")  # Ex: 'data-exp'
    dir_name = f"path-collection-{name}"

    scratch = os.path.join("/scratch/", getpass.getuser(), dir_name)
    if not os.path.exists(scratch):
        os.makedirs(scratch)

    return scratch


def make_executable(path_to_virtualenv: str) -> str:
    """Make executable script."""
    fpath = "./path_collector_env.sh"
    logging.info(f"Writing executable ({fpath})...")

    virtualenv = os.path.join(path_to_virtualenv, "bin/activate")
    logging.debug(f"Including Path to Python Virtual Env: {virtualenv}")

    with open(fpath, "w") as file:
        file.write(
            f"""#!/bin/bash
eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/setup.sh`
. {virtualenv}
$SROOT/metaprojects/combo/stable/env-shell.sh $@
"""
        )

    return fpath


def make_condor_file(  # pylint: disable=R0913,R0914
    scratch: str,
    prev_traverse: str,
    traverse_root: str,
    cpus: int,
    memory: str,
    chunk_size: int,
    excluded_paths: List[str],
    fast_forward: bool,
    accounting_group: str,
    path_to_virtualenv: str,
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
        ]
        # optional args
        previous_arg = f"--previous-traverse {prev_traverse}" if prev_traverse else ""
        exculdes_args = " ".join(excluded_paths) if excluded_paths else ""
        chunk_size_arg = f"--chunk-size {chunk_size}" if chunk_size else ""
        fast_forward_arg = "--fast-forward" if fast_forward else ""

        accounting_group_attr = (
            f'+AccountingGroup="{accounting_group}.{getpass.getuser()}"'
            if accounting_group
            else ""
        )

        # write
        file.write(
            f"""executable = {os.path.abspath(make_executable(path_to_virtualenv))}
arguments = python path_collector.py {traverse_root} --staging-dir {staging_dir} --workers {cpus} {previous_arg} --exclude {exculdes_args} {chunk_size_arg} {fast_forward_arg}
output = {scratch}/path_collector.out
error = {scratch}/path_collector.err
log = {scratch}/path_collector.log
+FileSystemDomain = "blah"
should_transfer_files = YES
transfer_input_files = {",".join([os.path.abspath(f) for f in transfer_input_files])}
request_cpus = {cpus}
{accounting_group_attr}
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
    parser.add_argument(
        "--accounting-group",
        default="",
        help="the accounting group to use, ex: 1_week. "
        "By default no accounting group is used.",
    )
    parser.add_argument(
        "--path-to-virtualenv",
        type=get_full_path,
        required=True,
        help="an NPX-accessible path to the python virtual environment",
    )
    parser.add_argument("--cpus", type=int, help="number of CPUs", default=8)
    parser.add_argument("--memory", help="amount of memory", default="20GB")
    args = parser.parse_args()

    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    # make condor scratch directory
    scratch = make_condor_scratch_dir(args.traverse_root)

    # make condor file
    condorpath = make_condor_file(
        scratch,
        args.previous_traverse,
        args.traverse_root,
        args.cpus,
        args.memory,
        args.chunk_size,
        args.exclude,
        args.fast_forward,
        args.accounting_group,
        args.path_to_virtualenv,
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
