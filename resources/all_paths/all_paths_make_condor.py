"""Make the Condor script for all_paths.py."""

import argparse
import getpass
import os
import subprocess
from typing import List, Optional

import bitmath  # type: ignore[import]


def _parse_to_bytes(size: str) -> int:
    return int(bitmath.parse_string_unsafe(size).to_Byte())


def _full_path(path: str) -> str:
    if not path:
        return path

    full_path = os.path.abspath(path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(full_path)

    return full_path


def make_condor_scratch_dir(traverse_root: str, with_exclusions: bool = False) -> str:
    """Make the condor scratch directory."""
    name = traverse_root.strip("/").replace("/", "-")  # Ex: 'data-exp'
    dir_name = f"all-paths-{name}"
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
    chunk_size: Optional[int],
    excluded_paths: List[str],
) -> str:
    """Make the condor file."""
    condorpath = os.path.join(scratch, "condor")
    with open(condorpath, "w") as file:
        # args
        staging_dir = os.path.join("/data/user/", getpass.getuser())
        transfer_input_files = ["all_paths.py", "traverser.py"]
        # optional args
        previous_arg = f"--previous-traverse {prev_traverse}" if prev_traverse else ""
        exculdes_args = " ".join(excluded_paths) if excluded_paths else ""
        chunk_size_arg = f"--chunk-size {chunk_size}" if chunk_size else ""

        # write
        file.write(
            f"""executable = {os.path.abspath('../indexer_env.sh')}
arguments = python all_paths.py {traverse_root} --staging-dir {staging_dir} --workers {cpus} {previous_arg} --exclude {exculdes_args} {chunk_size_arg}
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
        "recursively find all filepaths in `traverse_root`, "
        "place all_paths.py's output files in /data/user/{user}/, and "
        "Condor log files in /scratch/{user}/all-paths-{traverse_root_w_dashes}/."
    )
    parser.add_argument(
        "traverse_root",
        help="root directory to recursively scan for files.",
        type=_full_path,
    )
    parser.add_argument(
        "--previous-traverse",
        dest="prev_traverse",
        type=_full_path,
        help="prior file with file paths, eg: /data/user/eevans/data-exp-2020-03-10T15:11:42."
        " These files will be skipped.",
    )
    parser.add_argument(
        "--exclude",
        "-e",
        nargs="*",
        default=[],
        type=_full_path,
        help='directories/paths to exclude from the traverse -- keep it short, this is "all paths" after all.',
    )
    parser.add_argument(
        "--dryrun",
        default=False,
        action="store_true",
        help="does everything except submitting the condor job(s)",
    )
    parser.add_argument("--cpus", type=int, help="number of CPUs", default=8)
    parser.add_argument("--memory", help="amount of memory", default="20GB")
    parser.add_argument(
        "--chunk-size",
        dest="chunk_size",
        type=_parse_to_bytes,
        default=None,
        help="aggregate file-size limit per chunk/job (bytes, KB, MB, ...), by default, one chunk is made.",
    )
    args = parser.parse_args()

    for arg, val in vars(args).items():
        print(f"{arg}: {val}")

    # make condor scratch directory
    scratch = make_condor_scratch_dir(args.traverse_root, bool(args.exclude))

    # make condor file
    condorpath = make_condor_file(
        scratch,
        args.prev_traverse,
        args.traverse_root,
        args.cpus,
        args.memory,
        args.chunk_size,
        args.exclude,
    )

    # Execute
    if args.dryrun:
        print(f"Script Aborted: Condor job not submitted ({condorpath}).")
    else:
        cmd = f"condor_submit {condorpath}"
        print(cmd)
        subprocess.check_call(cmd.split(), cwd=scratch)


if __name__ == "__main__":
    main()
