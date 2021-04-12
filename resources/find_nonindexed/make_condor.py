"""Simple script to submit condor job."""


import argparse
import logging
import os
import subprocess

import coloredlogs  # type: ignore[import]

# Check CWD
if not os.getcwd().endswith("/find_nonindexed"):
    raise Exception("Must run script from find_nonindexed/")


# Constants
PY_SCRIPT = "get_nonindexed_files.py"
SCRATCH = "/scratch/eevans/nonindexed_condor"
CONDORPATH = os.path.join(SCRATCH, "condor")
ENV_EXCUTABLE = "./nonindexed_env.sh"
MEMORY = "20GB"
TRAVERSE_FILE = "/data/user/eevans/data-sim-2020-12-03T14:11:32"
LOG_LEVEL = "warning"


# Arguments
parser = argparse.ArgumentParser(
    description=f"Submit condor job for {PY_SCRIPT}",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--path-to-virtualenv",
    required=True,
    help="an NPX-accessible path to the python virtual environment",
)
parser.add_argument("--token", help="REST token for File Catalog", required=True)
parser.add_argument(
    "--dryrun",
    default=False,
    action="store_true",
    help="do everything except submitting the condor job(s)",
)
parser.add_argument("--cpus", type=int, help="number of cpus", required=True)
parser.add_argument("--threads", type=int, help="number of threads", required=True)
args = parser.parse_args()
coloredlogs.install(level="DEBUG")
for arg, val in vars(args).items():
    logging.warning(f"{arg}: {val}")


# Write Environment Executable
if not args.path_to_virtualenv.endswith("/bin/activate"):
    raise Exception("--path-to-virtualenv must end with /bin/activate")
with open(ENV_EXCUTABLE, "w") as file:
    logging.info(f"Writing {ENV_EXCUTABLE}...")
    file.write(
        f"""#!/bin/bash
eval `/cvmfs/icecube.opensciencegrid.org/py3-v4.1.0/setup.sh`
. {os.path.abspath(args.path_to_virtualenv)}
$@
"""
    )


# Write Condor
os.makedirs(SCRATCH)
with open(CONDORPATH, "w") as file:
    logging.info(f"Writing {CONDORPATH}...")
    file.write(
        f"""executable = {os.path.abspath(ENV_EXCUTABLE)}
arguments = python {PY_SCRIPT} -t {args.token} --traverse-file {TRAVERSE_FILE} --log {LOG_LEVEL} --threads {args.threads}
output = {SCRATCH}/nonindexed.out
error = {SCRATCH}/nonindexed.err
log = {SCRATCH}/nonindexed.log
+FileSystemDomain = "blah"
should_transfer_files = YES
transfer_input_files = {",".join([os.path.abspath(f) for f in [PY_SCRIPT]])}
request_cpus = {args.cpus}
request_memory = {MEMORY}
notification = Error
+AccountingGroup="2_week.$ENV(USER)"
queue
"""
    )


# Execute
if args.dryrun:
    logging.critical("Aborted: Condor job(s) not submitted.")
else:
    cmd = f"condor_submit {CONDORPATH}"
    logging.info(cmd)
    subprocess.check_call(cmd.split(), cwd=SCRATCH)
