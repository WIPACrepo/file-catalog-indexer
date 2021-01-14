"""Interface with IceProd REST interface.

Based on https://github.com/WIPACrepo/iceprod/blob/master/resources/get_file_info.py.
"""


# pylint: disable=R0903


import logging  # TODO - trim down
from typing import cast, Dict, List, Optional, Tuple, TypedDict, Union

# local imports
from iceprod.core import dataclasses  # type: ignore[import]
from iceprod.core.parser import ExpParser  # type: ignore[import]
from iceprod.core.serialization import dict_to_dataclasses  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

from ...utils import types, utils

_ICEPROD_V2_DATASET_RANGE = range(20000, 30000)
_ICEPROD_V1_DATASET_RANGE = range(0, 20000)


class _FileData(TypedDict):
    url: str
    iters: int
    task: str


class DatasetNotFound(Exception):
    """Raise when an IceProd dataset cannot be found."""


class TaskNotFound(Exception):
    """Raise when an IceProd task cannot be found."""


def parse_dataset_num(filepath: str) -> int:
    """Return the dataset num by parsing the filepath."""
    for dataset_range in [_ICEPROD_V2_DATASET_RANGE, _ICEPROD_V1_DATASET_RANGE]:
        parts = filepath.split("/")
        for p in parts[:-1]:
            try:
                dataset_num = int(p)
                if dataset_num in dataset_range:
                    return dataset_num
            except ValueError:
                continue
    raise DatasetNotFound(f"Could not determine dataset number: {filepath}")


class _IceProdQuerier:
    """Manage IceProd queries."""

    def __init__(self, dataset_num: int, rest_client: RestClient):
        self.dataset_num = dataset_num
        self.rest_client = rest_client

    async def get_dataset_config(self) -> dataclasses.Job:
        """Get the dataset's config dict, aka `dataclasses.Job`."""
        raise NotImplementedError()

    async def get_file_info(
        self,
        file: utils.FileInfo,
        config: dataclasses.Job,
        job_index: Optional[int] = None,
    ) -> types.IceProdMetadata:
        """Get IceProd Metadata for the dataset/job/task."""
        raise NotImplementedError()


class _IceProdV1Querier(_IceProdQuerier):
    """Manage IceProd v1 queries."""


class _IceProdV2Querier(_IceProdQuerier):
    """Manage IceProd v2 queries."""

    async def _get_dataset_info(self) -> Tuple[str, int]:
        dataset_id = ""
        datasets = await self.rest_client.request(
            "GET", "/datasets?keys=dataset_id|dataset|jobs_submitted"
        )
        for dataset_id in datasets:
            if datasets[dataset_id]["dataset"] == self.dataset_num:
                jobs_submitted = datasets[dataset_id]["jobs_submitted"]
                break
        else:
            raise DatasetNotFound(f"dataset num {self.dataset_num} not found")
        logging.info(f"dataset_id: {dataset_id}")
        return dataset_id, jobs_submitted

    async def get_dataset_config(self) -> dataclasses.Job:
        dataset_id, jobs_submitted = await self._get_dataset_info()

        config = await self.rest_client.request("GET", f"/config/{dataset_id}")
        config = dict_to_dataclasses(config)

        config["options"].update(
            {
                "dataset": self.dataset_num,
                "dataset_id": dataset_id,
                "jobs_submitted": jobs_submitted,
            }
        )

    @staticmethod
    def _get_output_files_data(config: dataclasses.Job) -> List[_FileData]:
        files: List[_FileData] = []
        # Search tasks' data
        for task in config["tasks"]:
            for task_d in task["data"]:
                if task_d["type"] in ("permanent", "site_temp") and task_d[
                    "movement"
                ] in ("output", "both",):
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

    async def _get_metadata(self, config: dataclasses.Job) -> types.IceProdMetadata:
        ret = await self.rest_client.request(
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
            raise TaskNotFound(
                "cannot get task info"
            )  # FIXME - what about non-matches?

        # pack & return
        config_url = f'https://iceprod2.icecube.wisc.edu/config?dataset_id={config["options"]["dataset_id"]}'
        data: types.IceProdMetadata = {
            "dataset": config["options"]["dataset"],  # int
            "dataset_id": config["options"]["dataset_id"],  # str
            "job": config["options"]["job"],  # int
            "job_id": task["job_id"],  # str
            "task": task["name"],  # str
            "task_id": task["task_id"],  # str
            "config": config_url,  # str
        }
        return data

    @staticmethod
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
        self,
        file: utils.FileInfo,
        config: dataclasses.Job,
        job_index: Optional[int] = None,
    ) -> types.IceProdMetadata:
        out_files_data = self._get_output_files_data(config)
        await self._add_file_data_to_config(
            file.path, out_files_data, config, job_index
        )

        return await self._get_metadata(config)


def _get_iceprod_querier(dataset_num: int, iceprodv2_rc: RestClient) -> _IceProdQuerier:
    if dataset_num in _ICEPROD_V1_DATASET_RANGE:
        return _IceProdV1Querier(dataset_num, None)  # TODO
    if dataset_num in _ICEPROD_V2_DATASET_RANGE:
        return _IceProdV2Querier(dataset_num, iceprodv2_rc)
    raise DatasetNotFound(f"Dataset Num ({dataset_num}) is undefined.")


async def get_dataset_config(
    dataset_num: int, iceprodv2_rc: RestClient, file: utils.FileInfo,
) -> Tuple[dataclasses.Job, int]:
    """Get config dict for the dataset."""
    try:
        ip_querier = _get_iceprod_querier(dataset_num, iceprodv2_rc)
    except DatasetNotFound:  # if given dataset num doesn't work try parsing from filepath
        dataset_num = parse_dataset_num(file.path)
        ip_querier = _get_iceprod_querier(dataset_num, iceprodv2_rc)

    return await ip_querier.get_dataset_config(), dataset_num


async def get_file_info(
    dataset_num: int,
    iceprodv2_rc: RestClient,
    file: utils.FileInfo,
    config: dataclasses.Job,
    job_index: Optional[int] = None,
) -> types.IceProdMetadata:
    """Get IceProd Metadata via REST."""
    ip_querier = _get_iceprod_querier(dataset_num, iceprodv2_rc)

    return await ip_querier.get_file_info(file, config, job_index)


def get_steering_paramters(
    config: dataclasses.Job,
) -> Dict[str, Union[str, float, int]]:
    """Return the steering parameters dict with macros resolved."""
    # TODO - IceProd 1?
    params = ExpParser().parse(
        config["steering"]["parameters"],
        config,
        {"parameters": config["steering"]["parameters"]},
    )

    return cast(Dict[str, Union[str, float, int]], params)
