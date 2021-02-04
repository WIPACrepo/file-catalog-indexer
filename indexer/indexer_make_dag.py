"""Make the Condor/DAGMan script for indexing files."""

import argparse
import getpass
import logging
import os
import re
import subprocess
from typing import cast, List

import coloredlogs  # type: ignore[import]
import natsort  # type: ignore[import]

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


# --------------------------------------------------------------------------------------
# Types


class IndexerArgs(TypedDict):
    """Arguments for indexer.py."""

    path_to_indexer: str
    blacklist: str
    token: str
    timeout: int
    retries: int
    cpus: int
    iceprodv2_rc_token: str
    iceprodv1_db_pass: str


# --------------------------------------------------------------------------------------
# Functions


def _scan_dir_of_paths_files(dir_of_paths_files: str) -> List[str]:
    fullpaths = [os.path.abspath(p.path) for p in os.scandir(dir_of_paths_files)]

    return cast(List[str], natsort.natsorted(fullpaths))


def make_condor_scratch_dir() -> str:
    """Make the condor scratch directory."""
    scratch = os.path.join("/scratch/", getpass.getuser(), "bulk-indexer")
    if not os.path.exists(scratch):
        os.makedirs(scratch)

    return scratch


def make_condor_file(scratch: str, memory: str, indexer_args: IndexerArgs) -> None:
    """Make the condor file."""
    logging.debug("Writing Condor file...")

    condorpath = os.path.join(scratch, "condor")
    if os.path.exists(condorpath):
        logging.warning(
            f"Writing Bypassed: {condorpath} already exists. Using preexisting condor file."
        )
    else:
        with open(condorpath, "w") as file:
            # configure transfer_input_files
            transfer_input_files = ["../requirements.txt"]
            blacklist_arg = ""
            if indexer_args["blacklist"]:
                blacklist_arg = f"--blacklist {indexer_args['blacklist']}"
                transfer_input_files.append(indexer_args["blacklist"])

            # /data/sim/-type arguments
            if indexer_args["iceprodv1_db_pass"] and indexer_args["iceprodv2_rc_token"]:
                sim_args = f"--iceprodv1-db-pass {indexer_args['iceprodv1_db_pass']} --iceprodv2-rc-token {indexer_args['iceprodv2_rc_token']}"
            else:
                sim_args = ""

            # paths_file
            path_arg = "--paths-file $(PATHS_FILE)"

            # write
            file.write(
                f"""executable = {os.path.abspath('../resources/indexer_env.sh')}
arguments = python {os.path.abspath(indexer_args['path_to_indexer'])} -s WIPAC {path_arg} -t {indexer_args['token']} --timeout {indexer_args['timeout']} --retries {indexer_args['retries']} {blacklist_arg} --log info --processes {indexer_args['cpus']} {sim_args}
output = {scratch}/$(JOBNUM).out
error = {scratch}/$(JOBNUM).err
log = {scratch}/$(JOBNUM).log
+FileSystemDomain = "blah"
should_transfer_files = YES
transfer_input_files = {",".join([os.path.abspath(f) for f in transfer_input_files])}
request_cpus = {indexer_args['cpus']}
request_memory = {memory}
notification = Error
queue
"""
            )
        logging.info(f"Finished writing Condor file @ {condorpath}.")


def make_dag_file(scratch: str, dir_of_paths_files: str) -> str:
    """Make the DAG file."""
    logging.debug("Writing DAG file...")

    dagpath = os.path.join(scratch, "dag")
    if os.path.exists(dagpath):
        logging.warning(
            f"Writing Bypassed: {dagpath} already exists. Using preexisting DAG file."
        )
    else:
        # write
        with open(dagpath, "w") as file:
            paths = _scan_dir_of_paths_files(dir_of_paths_files)

            start = 0
            if re.match(r".*[^\d]1$", paths[0]):
                start = 1

            for i, path in enumerate(paths, start=start):
                file.write(f"JOB job{i} condor\n")
                file.write(f'VARS job{i} PATHS_FILE="{path}"\n')
                file.write(f'VARS job{i} JOBNUM="{i}"\n')

            logging.info(f"Queued {len(paths)} jobs in {dagpath}.")

    return dagpath


def main() -> None:
    """Prep and execute DAGMan job(s).

    Make scratch directory, condor file, and DAGMan file.
    """
    if not os.getcwd().endswith("/file-catalog-indexer/indexer"):
        raise RuntimeError(
            "You must run this script from"
            " `file-catalog-indexer/indexer`."
            " This script uses relative paths."
        )

    parser = argparse.ArgumentParser(
        description="Submit HTCondor DAGMan jobs for bulk indexing files for the File Catalog",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--path-to-indexer",
        required=True,
        help="an NPX-accessible path to indexer.py"
        " (with additional necessary python files adjacent)",
    )
    parser.add_argument(
        "-t", "--token", help="REST token for File Catalog", required=True
    )
    parser.add_argument("-j", "--maxjobs", default=500, help="max concurrent jobs")
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="timeout duration (seconds) for File Catalog REST requests",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=10,
        help="number of retries for File Catalog REST requests",
    )
    parser.add_argument("--cpus", type=int, help="number of CPUs per job", default=2)
    parser.add_argument(
        "--memory", type=int, help="amount of memory (MB)", default=2000
    )
    parser.add_argument(
        "--dir-of-paths-files",
        required=True,
        help="the directory containing files, each file contains a collection of "
        "filepaths to index by a single job. Ex: /data/user/eevans/pre-index-data-exp/paths/",
    )
    parser.add_argument(
        "--blacklist",
        help="blacklist file containing all filepaths/directories to skip",
    )
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="do everything except submitting the condor job(s)",
    )
    parser.add_argument("--iceprodv2-rc-token", default="", help="IceProd2 REST token")
    parser.add_argument("--iceprodv1-db-pass", default="", help="IceProd1 SQL password")

    args = parser.parse_args()
    for arg, val in vars(args).items():
        logging.warning(f"{arg}: {val}")

    # check simulation-type args -> both or neither is OK
    if (args.iceprodv1_db_pass and not args.iceprodv2_rc_token) or (
        not args.iceprodv1_db_pass and args.iceprodv2_rc_token
    ):
        raise RuntimeError(
            "Must use both --iceprodv1-db-pass & --iceprodv2-rc-token, or neither."
        )

    # check paths in args
    for fpath in [args.blacklist, args.dir_of_paths_files, args.path_to_indexer]:
        if fpath and not os.path.exists(fpath):
            raise FileNotFoundError(fpath)

    # make condor scratch directory
    scratch = make_condor_scratch_dir()

    # make condor file
    indexer_args: IndexerArgs = {
        "path_to_indexer": args.path_to_indexer,
        "blacklist": args.blacklist,
        "token": args.token,
        "timeout": args.timeout,
        "retries": args.retries,
        "cpus": args.cpus,
        "iceprodv2_rc_token": args.iceprodv2_rc_token,
        "iceprodv1_db_pass": args.iceprodv1_db_pass,
    }
    make_condor_file(scratch, args.memory, indexer_args)

    # make DAG file
    dagpath = make_dag_file(scratch, args.dir_of_paths_files)

    # Execute
    if args.dryrun:
        logging.critical("Indexer Aborted: Condor jobs not submitted.")
    else:
        cmd = f"condor_submit_dag -maxjobs {args.maxjobs} {dagpath}"
        logging.info(cmd)
        subprocess.check_call(cmd.split(), cwd=scratch)


if __name__ == "__main__":
    coloredlogs.install(level="DEBUG")
    main()
