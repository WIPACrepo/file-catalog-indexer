"""Interface with IceProd REST interface.

Based on https://github.com/WIPACrepo/iceprod/blob/master/resources/get_file_info.py.
"""


import logging
from typing import List, Optional, Tuple, TypedDict

# local imports
from iceprod.core import dataclasses  # type: ignore[import]
from iceprod.core.parser import ExpParser  # type: ignore[import]
from iceprod.core.serialization import dict_to_dataclasses  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

from ...utils import types


class _FileData(TypedDict):
    url: str
    iters: int
    task: str


class DatasetNotFound(Exception):
    """Raise when an IceProd dataset cannot be found."""


class TaskNotFound(Exception):
    """Raise when an IceProd task cannot be found."""


def _get_config_url(dataset_id: str) -> str:
    return f"https://iceprod2.icecube.wisc.edu/config?dataset_id={dataset_id}"


def _get_dataset_num(filepath: str) -> int:
    parts = filepath.split("/")
    for p in parts[:-1]:
        try:
            dataset_num = int(p)
            if 20000 < dataset_num < 30000:
                break
        except ValueError:
            continue
    else:
        raise DatasetNotFound("could not determine dataset number")
    logging.info(f"dataset num: {dataset_num}")
    return dataset_num


async def _get_dataset_info(
    rest_client: RestClient, dataset_num: int
) -> Tuple[str, int]:
    dataset_id = ""
    datasets = await rest_client.request(
        "GET", "/datasets?keys=dataset_id|dataset|jobs_submitted"
    )
    for dataset_id in datasets:
        if datasets[dataset_id]["dataset"] == dataset_num:
            jobs_submitted = datasets[dataset_id]["jobs_submitted"]
            break
    else:
        raise DatasetNotFound(f"dataset num {dataset_num} not found")
    logging.info(f"dataset_id: {dataset_id}")
    return dataset_id, jobs_submitted


def _get_output_files_data(config: dataclasses.Job) -> List[_FileData]:
    files: List[_FileData] = []
    # Search tasks' data
    for task in config["tasks"]:
        for task_d in task["data"]:
            if task_d["type"] in ("permanent", "site_temp") and task_d["movement"] in (
                "output",
                "both",
            ):
                files.append(
                    {"url": task_d["remote"], "iters": 1, "task": task["name"]}
                )
        # Search trays' data
        for tray in task["trays"]:
            for tray_d in tray["data"]:
                if tray_d["type"] in ("permanent", "site_temp") and tray_d[
                    "movement"
                ] in ("output", "both"):
                    files.append(
                        {
                            "url": tray_d["remote"],
                            "iters": tray["iterations"],
                            "task": task["name"],
                        }
                    )
            # Search modules' data
            for module in tray["modules"]:
                for module_d in module["data"]:
                    if module_d["type"] in ("permanent", "site_temp") and module_d[
                        "movement"
                    ] in ("output", "both"):
                        files.append(
                            {
                                "url": module_d["remote"],
                                "iters": tray["iterations"],
                                "task": task["name"],
                            }
                        )
    return files


async def _get_metadata(
    rest_client: RestClient, config: dataclasses.Job
) -> types.IceProdMetadata:
    ret = await rest_client.request(
        "GET",
        f"/datasets/{config['options']['dataset_id']}/tasks",
        {
            "job_index": config["options"]["job"],
            "keys": "name|task_id|job_id|task_index",
        },
    )

    # find matching task
    task = {}
    for task in ret.values():
        if task["name"] == config["options"]["task"]:
            break
    else:
        raise TaskNotFound("cannot get task info")  # FIXME - what about non-matches?

    # pack & return
    data: types.IceProdMetadata = {
        "dataset": config["options"]["dataset"],
        "dataset_id": config["options"]["dataset_id"],
        "job": config["options"]["job"],
        "job_id": task["job_id"],
        "task": task["name"],
        "task_id": task["task_id"],
        "config": _get_config_url(config["options"]["dataset_id"]),
    }
    return data


async def _add_file_data_to_config(
    filepath: str,
    out_files_data: List[_FileData],
    config: dataclasses.Job,
    job_index: Optional[int],
) -> None:
    """Add `"task"`, `"job"`, & `"iter"` values to `config["options"]`."""
    if job_index:
        job_search: List[int] = [job_index]
    else:
        job_search = list(range(config["options"]["jobs_submitted"]))

    parser = ExpParser()
    env = {"parameters": config["steering"]["parameters"]}

    # search each file/task
    for f_data in reversed(out_files_data):
        logging.info(f'searching task {f_data["task"]}')
        config["options"]["task"] = f_data["task"]
        # search each job
        for job in job_search:
            config["options"]["job"] = job
            # search each iter
            for i in range(f_data["iters"]):
                config["options"]["iter"] = i
                url = parser.parse(f_data["url"], config, env)
                if "//" not in url:
                    path = url
                else:
                    path = "/" + url.split("//", 1)[1].split("/", 1)[1]
                logging.info(f"checking path {path}")
                if path == filepath:
                    logging.info(f"success on job_index: {job}, iter: {i}")
                    return

    raise Exception("no path match found")


async def get_file_info(
    rest_client: RestClient,
    filepath: str,
    config: dataclasses.Job,
    job_index: Optional[int] = None,
) -> types.IceProdMetadata:
    """Get IceProd Metadata via REST."""
    out_files_data = _get_output_files_data(config)
    await _add_file_data_to_config(filepath, out_files_data, config, job_index)

    return await _get_metadata(rest_client, config)


async def get_dataset_config(
    rest_client: RestClient, filepath: str, dataset_num: Optional[int]
) -> dataclasses.Job:
    """Get config dict for the dataset."""
    if not dataset_num:
        dataset_num = _get_dataset_num(filepath)
    dataset_id, jobs_submitted = await _get_dataset_info(rest_client, dataset_num)

    config = await rest_client.request("GET", f"/config/{dataset_id}")
    config = dict_to_dataclasses(config)
    config["options"].update(
        {
            "dataset": dataset_num,
            "dataset_id": dataset_id,
            "jobs_submitted": jobs_submitted,
        }
    )

    return config
