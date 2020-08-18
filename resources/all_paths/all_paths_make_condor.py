"""Make the Condor script for all_paths.py."""

import argparse
import getpass
import os
import subprocess


def make_condor_scratch_dir(paths_root: str) -> str:
    """Make the condor scratch directory."""
    name = paths_root.strip("/").replace("/", "-")  # Ex: 'data-exp'

    scratch = os.path.join("/scratch/", getpass.getuser(), f"all-paths-{name}")
    if not os.path.exists(scratch):
        os.makedirs(scratch)

    return scratch


def make_condor_file(
    scratch: str, previous_all_paths: str, paths_root: str, cpus: int, memory: str
) -> str:
    """Make the condor file."""
    condorpath = os.path.join(scratch, "condor")
    with open(condorpath, "w") as file:
        # args
        previous_arg = ""
        if previous_all_paths:
            previous_arg = f"--previous-all-paths {previous_all_paths}"
        staging_dir = os.path.join("/data/user/", getpass.getuser())
        transfer_input_files = ["all_paths.py", "directory_scanner.py"]

        # write
        file.write(
            f"""executable = {os.path.abspath('../indexer_env.sh')}
arguments = python all_paths.py {paths_root} --staging-dir {staging_dir} --workers {cpus} {previous_arg}
output = {scratch}/all_paths.out
error = {scratch}/all_paths.err
log = {scratch}/all_paths.log
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
    """Prep and execute Condor job (to run all_paths.py).

    Make scratch directory and condor file.
    """
    parser = argparse.ArgumentParser(
        description="Make Condor script for all_paths.py: "
        "recursively find all filepaths in `paths_root`, "
        "place all_paths.py's output files in /data/user/{user}/, and "
        "Condor log files in /scratch/{user}/all-paths-{paths_root_w_dashes}/."
    )
    parser.add_argument(
        "paths_root", help="root directory to recursively scan for files."
    )
    parser.add_argument(
        "--previous-all-paths",
        dest="previous_all_paths",
        help="prior file with file paths, eg: /data/user/eevans/data-exp-2020-03-10T15:11:42."
        " These files will be skipped.",
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
        print(f"{arg}: {val}")

    # check paths in args
    for path in [args.paths_root, args.previous_all_paths]:
        if path and not os.path.exists(path):
            raise FileNotFoundError(path)

    # make condor scratch directory
    scratch = make_condor_scratch_dir(args.paths_root)

    # make condor file
    condorpath = make_condor_file(
        scratch, args.previous_all_paths, args.paths_root, args.cpus, args.memory
    )

    # Execute
    if args.dryrun:
        print("Script Aborted: Condor job not submitted.")
    else:
        cmd = f"condor_submit {condorpath}"
        print(cmd)
        subprocess.check_call(cmd.split(), cwd=scratch)


if __name__ == "__main__":
    main()
