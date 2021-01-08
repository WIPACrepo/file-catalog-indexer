"""Interface with IceProd REST interface.

Based on https://github.com/WIPACrepo/iceprod/blob/master/resources/get_file_info.py.
"""


import logging
from typing import Any, cast, Dict, List, Optional, Tuple, TypedDict

# local imports
from iceprod.core.parser import ExpParser  # type: ignore[import]
from iceprod.core.serialization import dict_to_dataclasses  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

from ...utils import types


class _FileData(TypedDict):
    url: str
    iters: int
    task: str


class _ConfigOptions(TypedDict, total=False):
    dataset: int
    dataset_id: str
    job: int
    task: str
    jobs_submitted: int
    iter: int


class _Config(TypedDict):
    options: _ConfigOptions
    tasks: List[Any]
    steering: Dict[str, Any]


def _get_config_url(dataset_id: str) -> str:
    return f"https://iceprod2.icecube.wisc.edu/config?dataset_id={dataset_id}"


def _get_dataset_num(filename: str) -> int:
    parts = filename.split("/")
    for p in parts[:-1]:
        try:
            dataset_num = int(p)
            if 20000 < dataset_num < 30000:
                break
        except:  # noqa # pylint: disable=W0702
            continue
    else:
        raise Exception("could not determine dataset number")
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
        raise Exception(f"dataset num {dataset_num} not found")
    logging.info(f"dataset_id: {dataset_id}")
    return dataset_id, jobs_submitted


async def _get_config(rest_client: RestClient, dataset_id: str) -> _Config:
    config = await rest_client.request("GET", f"/config/{dataset_id}")
    config = dict_to_dataclasses(config)
    return cast(_Config, config)


def _get_output_files_data(config: _Config) -> List[_FileData]:
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
    rest_client: RestClient, config: _Config
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
        raise Exception("cannot get task info")  # FIXME - what about non-matches?

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


async def _add_file_data(
    filename: str,
    out_files_data: List[_FileData],
    config: _Config,
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
                if path == filename:
                    logging.info(f"success on job_index: {job}, iter: {i}")
                    return

    raise Exception("no path match found")


async def get_file_info(
    rest_client: RestClient,
    filename: str,
    dataset_num: Optional[int] = None,
    job_index: Optional[int] = None,
) -> types.IceProdMetadata:
    """Get IceProd Metadata via REST."""
    if not dataset_num:
        dataset_num = _get_dataset_num(filename)
    dataset_id, jobs_submitted = await _get_dataset_info(rest_client, dataset_num)
    config = await _get_config(rest_client, dataset_id)
    out_files_data = _get_output_files_data(config)

    config["options"].update(
        {
            "dataset": dataset_num,
            "dataset_id": dataset_id,
            "jobs_submitted": jobs_submitted,
        }
    )

    await _add_file_data(filename, out_files_data, config, job_index)
    return await _get_metadata(rest_client, config)
