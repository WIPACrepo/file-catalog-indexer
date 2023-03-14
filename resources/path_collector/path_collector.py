"""Traverse given directory for all filepaths, and split list into chunks.

These chunks are outputted files, which are used as input in
indexer_make_dag.py jobs.

NOTE - If this script is used regularly in the future,
consider breaking-up/refactoring the pipeline for usability.
Currently the process of fast-forwarding is convoluted if
the use case is anything other than a condor restart.
"""

import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime as dt
from typing import Any, List, Optional, TypedDict, Union

import bitmath  # type: ignore[import]
import coloredlogs  # type: ignore[import]

from resources.path_collector.common_args import (  # isort:skip  # noqa # pylint: disable=E0401,C0413,C0411
    get_parser_w_common_args,
    get_full_path,
)


def check_call_and_log(
    cmd: Union[List[str], str], cwd: str = ".", shell: bool = False
) -> None:
    """Wrap subprocess.check_call and print command."""
    if shell and isinstance(cmd, list):
        raise Exception("Do not set shell=True and pass a list--pass a string.")
    logging.debug(f"Execute: {cmd} @ {cwd}")
    subprocess.check_call(cmd, cwd=cwd, shell=shell)


def _call_traverser(
    traverse_staging_dir: str,
    traverse_root: str,
    excluded_paths: List[str],
    workers: int,
) -> str:
    """Get all filepaths in traverse_root."""
    traverser_log = os.path.join(traverse_staging_dir, "traverser.log")
    traverse_raw_tmp = os.path.join(traverse_staging_dir, "traverse.raw.tmp")

    # copy over existing traverser.log
    if os.path.exists(traverser_log):
        dst = os.path.join(
            traverse_staging_dir,
            f"old-traverser-log-{dt.now().isoformat(timespec='seconds')}",
        )
        os.rename(traverser_log, dst)

    # traverse
    excludes_args = "--exclude " + " ".join(excluded_paths) if excluded_paths else ""
    check_call_and_log(
        f"python3 -m resources.path_collector.traverser {traverse_root} "
        f"--workers {workers}"
        f" {excludes_args} > {traverse_raw_tmp} 2> {traverser_log}",
        shell=True,
    )

    # mark as finished
    traverse_raw = os.path.join(traverse_staging_dir, "traverse.raw")
    os.rename(traverse_raw_tmp, traverse_raw)

    logging.info("Finished traversing.")
    return traverse_raw


def _file_sorted(traverse_staging_dir: str, fname: str) -> str:
    """Make a sorted copy.

    This'll ensure chunks/jobs have filepaths from the same 'region'.
    """
    traverse_sorted_tmp = os.path.join(traverse_staging_dir, "traverse.sorted.tmp")

    # remove blanks
    check_call_and_log(f"""sed -i '/^[[:space:]]*$/d' {fname}""", shell=True)

    # sort
    check_call_and_log(
        f"sort -T {traverse_staging_dir} {fname} > {traverse_sorted_tmp}", shell=True
    )

    # mark as finished
    traverse_sorted = os.path.join(traverse_staging_dir, "traverse.sorted")
    os.rename(traverse_sorted_tmp, traverse_sorted)

    logging.info("Finished sorting.")
    return traverse_sorted


def _get_unique_lines(traverse_staging_dir: str, prev_traverse: str, fname: str) -> str:
    """Get lines(filepaths) unique to this traverse versus the previous."""
    traverse_unique_tmp = os.path.join(traverse_staging_dir, "traverse.unique.tmp")
    traverse_unique = os.path.join(traverse_staging_dir, "traverse.unique")
    msg = " (nothing to compare to)"

    if prev_traverse:
        msg = ""
        check_call_and_log(
            f"comm -1 -3 {prev_traverse} {fname} > {traverse_unique_tmp}",
            shell=True,
        )
        # mark as finished
        os.rename(traverse_unique_tmp, traverse_unique)
    else:
        # mark as finished
        shutil.copyfile(fname, traverse_unique)

    logging.info(f"Finished getting unique lines{msg}.")
    return traverse_unique


def _get_chunks_dir(traverse_staging_dir: str) -> str:
    return os.path.join(traverse_staging_dir, "traverse-chunks/")


def _chunk(traverse_staging_dir: str, chunk_size: int, traverse_file: str) -> None:
    """Chunk the traverse file up by approx equal aggregate file size.

    Assumes: `chunk_size` >> any one file's size

    Chunks are guaranteed to be equal to or barely greater than
    `chunk_size`. If `chunk_size` is too small (< `MINIMUM_CHUNK_SIZE`),
    only one chunk is made ("chunk-0"), a copy of `traverse_file`.

    Example:
    `traverse_staging_dir/chunks/chunk-1645`
    """
    chunks_dir = _get_chunks_dir(traverse_staging_dir)

    check_call_and_log(f"mkdir {chunks_dir}".split())

    if chunk_size == 0:
        logging.warning("Chunking bypassed, --chunk-size is zero")
        check_call_and_log(
            f"cp {traverse_file} {os.path.join(chunks_dir, 'chunk-0')}".split()
        )
        return

    class _Chunk(TypedDict):
        id_: int
        size: int
        lines: List[str]

    def _write_chunk_file(chunk: _Chunk) -> str:
        fname = f"chunk-{chunk['id_']}"
        with open(os.path.join(chunks_dir, fname), "w") as chunk_f:
            chunk_f.writelines(chunk["lines"])
        return fname

    chunk: _Chunk = {"id_": 1, "size": 0, "lines": []}
    total_f_size = 0
    with open(traverse_file, "r") as f:
        for fpath_line in f:
            try:
                f_size = int(os.stat(fpath_line.strip()).st_size)
            except FileNotFoundError:
                logging.warning(
                    f"Skipping file '{fpath_line.strip()}'--path was removed since traversal."
                )
                continue
            # append & increment
            chunk["lines"].append(fpath_line)
            chunk["size"] += f_size
            total_f_size += f_size
            # time to chunk?
            if chunk["size"] >= chunk_size:
                _write_chunk_file(chunk)
                # reset for next chunking
                next_id = chunk["id_"] + 1
                chunk = {"id_": next_id, "size": 0, "lines": []}
    # chunk whatever is left over
    if chunk["lines"]:
        _write_chunk_file(chunk)

    logging.info(
        f"Chunked traverse into {chunk['id_']} chunk-files"
        f" ~{bitmath.best_prefix(chunk_size).format('{value:.2f} {unit}')}"
        f" ({chunk_size} bytes) each @ {chunks_dir}."
        f" Total ~{bitmath.best_prefix(total_f_size).format('{value:.2f} {unit}')}."
    )


def _archive(staging_dir: str, suffix: str, traverse_file: str) -> str:
    """Copy/Archive traverse into a file.

    Example:
    /data/user/eevans/data-exp-2020-03-10T15:11:42
    """
    time = dt.now().isoformat(timespec="seconds")
    file_archive = os.path.join(staging_dir, f"{suffix}-{time}")

    check_call_and_log(f"cp {traverse_file} {file_archive}".split())

    logging.info(f"Finished archiving: at {file_archive}.")
    return file_archive


def _suffix(traverse_root: str) -> str:
    return traverse_root.strip("/").replace("/", "-")  # Ex: 'data-exp'


def _get_traverse_staging_dir(staging_dir: str, traverse_root: str) -> str:
    return os.path.join(staging_dir, f"pre-index-{_suffix(traverse_root)}/")


def write_all_filepaths_to_files(  # pylint: disable=R0913
    staging_dir: str,
    traverse_root: str,
    workers: int,
    prev_traverse: str,
    chunk_size: int,
    excluded_paths: List[str],
    ff_traverse_file: Optional[str],
) -> None:
    """Write all filepaths (rooted from `traverse_root`) to multiple files."""
    traverse_staging_dir = _get_traverse_staging_dir(staging_dir, traverse_root)

    # traverse_staging_dir must already exist
    if not os.path.exists(traverse_staging_dir):
        raise FileNotFoundError(traverse_staging_dir)

    # output argv to a file
    with open(os.path.join(traverse_staging_dir, "argv.txt"), "a") as f:
        f.write(" ".join(sys.argv) + "\n")

    suffix = _suffix(traverse_root)

    # figure out where to start...
    if not ff_traverse_file:
        # get traverse.raw
        fname = _call_traverser(
            traverse_staging_dir, traverse_root, excluded_paths, workers
        )
        # get traverse.sorted
        fname = _file_sorted(traverse_staging_dir, fname)
        # get traverse.unique
        fname = _get_unique_lines(traverse_staging_dir, prev_traverse, fname)
        # get <archive-file>
        fname = _archive(staging_dir, suffix, fname)

    elif ff_traverse_file.endswith(".raw"):
        logging.warning("Fast-forwarding past traversing...")
        # get traverse.sorted
        fname = _file_sorted(traverse_staging_dir, ff_traverse_file)
        # get traverse.unique
        fname = _get_unique_lines(traverse_staging_dir, prev_traverse, fname)
        # get <archive-file>
        fname = _archive(staging_dir, suffix, fname)

    elif ff_traverse_file.endswith(".sorted"):
        logging.warning("Fast-forwarding past sorting...")
        # get traverse.unique
        fname = _get_unique_lines(traverse_staging_dir, prev_traverse, ff_traverse_file)
        # get <archive-file>
        fname = _archive(staging_dir, suffix, fname)

    elif ff_traverse_file.endswith(".unique"):
        logging.warning("Fast-forwarding to traverse archiving...")
        # get <archive-file>
        fname = _archive(staging_dir, suffix, ff_traverse_file)

    else:
        raise RuntimeError(f"Unknown type of fast-forward file {ff_traverse_file}")

    logging.info(f"Chunking {fname}...")
    _chunk(traverse_staging_dir, chunk_size, fname)

    # cleanup
    logging.warning("Cleaning up. Deleting traverse.* files...")
    for file in ["traverse.unique", "traverse.sorted", "traverse.raw"]:
        if file in [os.path.basename(p) for p in os.listdir(traverse_staging_dir)]:
            os.remove(os.path.join(traverse_staging_dir, file))


def _get_path_collector_log(traverse_staging_dir: str) -> str:
    return os.path.join(traverse_staging_dir, "path_collector.log")


def _figure_fast_forwarding(traverse_staging_dir: str) -> str:
    # was using `--fast-forward` moot?
    if not os.path.exists(traverse_staging_dir):
        logging.info(
            f"--fast-forwarding was not needed,"
            f" there was no existing {traverse_staging_dir}"
        )
        os.mkdir(traverse_staging_dir)
        return ""

    logging.info("Figuring where to fast-forward to...")

    # clean-up for fast forwarding -- rm -r chunks/ && rm *.tmp
    logging.warning("Cleaning up traverse-staging directory:")
    # ls
    logging.info(
        f"BEFORE: ls {traverse_staging_dir} -> {os.listdir(traverse_staging_dir)}"
    )
    # rm -r chunks/
    if os.path.exists(_get_chunks_dir(traverse_staging_dir)):
        logging.warning(f"rm -r {_get_chunks_dir(traverse_staging_dir)}...")
        shutil.rmtree(_get_chunks_dir(traverse_staging_dir))
    # rm *.tmp
    for fpath in [
        os.path.join(traverse_staging_dir, fn)
        for fn in os.listdir(traverse_staging_dir)
    ]:
        if fpath.endswith(".tmp"):
            logging.debug(f"rm {fpath}...")
            os.remove(fpath)
    # ls
    logging.info(
        f"AFTER: ls {traverse_staging_dir} -> {os.listdir(traverse_staging_dir)}"
    )

    # find which traverse.* file to use for fast-forwarding
    for fname in ["traverse.unique", "traverse.sorted", "traverse.raw"]:
        if fname in os.listdir(traverse_staging_dir):
            logging.info(f"Found the last completed traverse.* file: '{fname}'")
            return os.path.join(traverse_staging_dir, fname)
    # ---
    logging.info("Cannot fast-forward -- there were no complete traverse.* files")
    return ""


def _try_log_setup(args: Any) -> None:
    """Try to set up logging.

    If there is no traverse-staging directory this will fail.
    """
    traverse_staging_dir = _get_traverse_staging_dir(
        args.staging_dir, args.traverse_root
    )

    try:
        coloredlogs.install(level="DEBUG")
        # also log to a file -- use the formatter (and level) from coloredlogs
        fhandler = logging.FileHandler(_get_path_collector_log(traverse_staging_dir))
        fhandler.setFormatter(logging.getLogger().handlers[0].formatter)
        logging.getLogger().addHandler(fhandler)
        # log args
        for arg, val in vars(args).items():
            logging.warning(f"{arg}: {val}")
    except FileNotFoundError:
        pass


def main() -> None:
    """Get all filepaths rooted at directory and split-up/write to files."""
    parser = get_parser_w_common_args(
        "Run this script via path_collector_make_condor.py."
    )
    parser.add_argument(
        "--staging-dir",
        dest="staging_dir",
        type=get_full_path,
        required=True,
        help="the base directory to store files for jobs, eg: /data/user/eevans/",
    )
    parser.add_argument(
        "--workers",
        type=int,
        help="max number of workers. **Potentially bypassed if also using --fast-forward**",
        required=True,
    )
    args = parser.parse_args()
    # print args
    for arg, val in vars(args).items():
        print(f"{arg}: {val}")

    _try_log_setup(args)

    #
    # figure the traverse-staging directory & where/if to fast-forward
    traverse_staging_dir = _get_traverse_staging_dir(
        args.staging_dir, args.traverse_root
    )
    if args.fast_forward:
        ff_traverse_file = _figure_fast_forwarding(traverse_staging_dir)
    elif os.path.exists(traverse_staging_dir):
        raise FileExistsError(f"{traverse_staging_dir}. Use --fast-forward?")
    else:
        ff_traverse_file = ""
        os.mkdir(traverse_staging_dir)

    _try_log_setup(args)  # try again -- traverse-staging directory for sure exists now

    #
    # traverse and chunk!
    write_all_filepaths_to_files(
        args.staging_dir,
        args.traverse_root,
        args.workers,
        args.previous_traverse,
        args.chunk_size,
        args.exclude,
        ff_traverse_file,
    )

    logging.info("Done.")


if __name__ == "__main__":
    main()
