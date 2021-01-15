"""Interface with IceProd REST interface.

Based on https://github.com/WIPACrepo/iceprod/blob/master/resources/get_file_info.py.
"""

# pylint: disable=R0903

import functools
import logging  # TODO - trim down
from typing import cast, Dict, List, Optional, Tuple, TypedDict, Union

import pymysql

# local imports
from iceprod.core import dataclasses  # type: ignore[import]
from iceprod.core.parser import ExpParser  # type: ignore[import]
from iceprod.core.serialization import dict_to_dataclasses  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

from ...utils import types

# --------------------------------------------------------------------------------------
# Constants

_ICEPROD_V2_DATASET_RANGE = range(20000, 30000)
_ICEPROD_V1_DATASET_RANGE = range(0, 20000)

_HTML_TAGS = []
for tag in ["b", "strong", "i", "em", "mark", "small", "del", "ins", "sub", "sup"]:
    _HTML_TAGS.extend([f"<{tag}>", f"</{tag}>"])


# --------------------------------------------------------------------------------------
# Types

SteeringParameters = Dict[str, Union[str, float, int]]


class _OutFileData(TypedDict):
    url: str
    iters: int
    task: str


class _IP2RESTDataset(TypedDict):
    dataset: int
    dataset_id: str
    jobs_submitted: int


class DatasetNotFound(Exception):
    """Raise when an IceProd dataset cannot be found."""


class TaskNotFound(Exception):
    """Raise when an IceProd task cannot be found."""


class OutFileNotFound(Exception):
    """Raise when an IceProd outfile cannot be found."""


# --------------------------------------------------------------------------------------
# Private Query Managers


class _IceProdQuerier:
    """Manage IceProd queries."""

    def __init__(self, dataset_num: int):
        self.dataset_num = dataset_num

    async def get_job_config(
        self, filepath: str, job_index: Optional[int]
    ) -> dataclasses.Job:
        """Get the job's config dict, AKA `dataclasses.Job`."""
        raise NotImplementedError()

    @staticmethod
    def _expand_steering_parameters(job_config: dataclasses.Job) -> None:
        job_config["steering"]["parameters"] = ExpParser().parse(
            job_config["steering"]["parameters"],
            job_config,
            {"parameters": job_config["steering"]["parameters"]},
        )


class _IceProdV1Querier(_IceProdQuerier):
    """Manage IceProd v1 queries."""

    def __init__(self, dataset_num: int, iceprodv1_db: pymysql.connections.Connection):
        super().__init__(dataset_num)
        self.iceprodv1_db = iceprodv1_db

    def _query_steering_params(self) -> SteeringParameters:
        steering_params = {}
        sql = (
            "SELECT * FROM steering_parameter "
            f"WHERE dataset_id = {self.dataset_num} "
            "ORDER by name"
        )

        cursor = self.iceprodv1_db.cursor()
        cursor.execute(sql)
        result_set = cursor.fetchall()

        if not result_set:
            raise DatasetNotFound()

        for param in result_set:
            value = param["value"]  # type: ignore[call-overload]
            for html_tag in _HTML_TAGS:
                value = value.replace(html_tag, "")
            steering_params[param["name"]] = value  # type: ignore[call-overload]

        return steering_params

    async def get_job_config(
        self, filepath: str, job_index: Optional[int]
    ) -> dataclasses.Job:
        steering_params = self._query_steering_params()

        job_config = dict_to_dataclasses(
            {
                "steering": {"parameters": steering_params},
                "options": {
                    "dataset": self.dataset_num,
                    "dataset_id": str(self.dataset_num),
                },
            }
        )

        # resolve/expand steering parameters
        self._expand_steering_parameters(job_config)

        return job_config


@functools.lru_cache()
async def _get_all_iceprod2_datasets(
    iceprodv2_rc: RestClient,
) -> Dict[str, _IP2RESTDataset]:
    datasets = await iceprodv2_rc.request(
        "GET", "/datasets?keys=dataset_id|dataset|jobs_submitted"
    )

    return cast(Dict[str, _IP2RESTDataset], datasets)


class _IceProdV2Querier(_IceProdQuerier):
    """Manage IceProd v2 queries."""

    def __init__(self, dataset_num: int, iceprodv2_rc: RestClient):
        super().__init__(dataset_num)
        self.iceprodv2_rc = iceprodv2_rc

    async def _get_dataset_info(self) -> Tuple[str, int]:
        datasets = await _get_all_iceprod2_datasets(self.iceprodv2_rc)

        dataset_id = ""
        for dataset_id in datasets:
            if datasets[dataset_id]["dataset"] == self.dataset_num:
                jobs_submitted = datasets[dataset_id]["jobs_submitted"]
                logging.info(f"dataset_id: {dataset_id}")
                return dataset_id, jobs_submitted

        raise DatasetNotFound(f"dataset num {self.dataset_num} not found")

    async def get_job_config(
        self, filepath: str, job_index: Optional[int]
    ) -> dataclasses.Job:
        dataset_id, jobs_submitted = await self._get_dataset_info()

        ret = await self.iceprodv2_rc.request("GET", f"/config/{dataset_id}")
        job_config: dataclasses.Job = dict_to_dataclasses(ret)

        job_config["options"].update(
            {
                "dataset": self.dataset_num,
                "dataset_id": dataset_id,
                "jobs_submitted": jobs_submitted,
            }
        )

        try:
            await self._match_outfile_and_add_to_job_config(
                filepath, job_config, job_index
            )
        except OutFileNotFound:
            logging.warning(f"Outfile ({filepath}) could not be matched.")

        try:
            await self._add_task_info_to_job_config(job_config)
        except TaskNotFound:
            logging.warning(f"Could not get task info for {filepath}")

        # resolve/expand steering parameters
        self._expand_steering_parameters(job_config)

        return job_config

    @staticmethod
    def _get_outfiles(job_config: dataclasses.Job) -> List[_OutFileData]:
        """Get every single outputted file, plus some data on each."""
        files: List[_OutFileData] = []
        # Search each task's data
        for task in job_config["tasks"]:
            for task_d in task["data"]:
                if task_d["type"] in ("permanent", "site_temp") and task_d[
                    "movement"
                ] in ("output", "both"):
                    files.append(
                        {"url": task_d["remote"], "iters": 1, "task": task["name"]}
                    )
            # Search each tray's data
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
                # Search each module's data
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

    async def _add_task_info_to_job_config(self, job_config: dataclasses.Job) -> None:
        """Add `"task_info"` dict to `job_config["options"]`."""
        if "task" not in job_config["options"]:
            raise TaskNotFound()

        ret = await self.iceprodv2_rc.request(
            "GET",
            f"/datasets/{job_config['options']['dataset_id']}/tasks",
            {
                "job_index": job_config["options"]["job"],
                "keys": "name|task_id|job_id|task_index",
            },
        )

        # find matching task
        task = {}
        for task in ret.values():
            if task["name"] == job_config["options"]["task"]:
                job_config["options"]["task_info"] = task
                return

        raise TaskNotFound()

    @staticmethod
    async def _match_outfile_and_add_to_job_config(
        filepath: str, job_config: dataclasses.Job, job_index: Optional[int],
    ) -> None:
        """Add `"task"`, `"job"`, & `"iter"` values to `config["options"]`."""
        if job_index:  # do we already know what job to look at?
            job_search: List[int] = [job_index]
        else:  # otherwise, look at each job from dataset
            job_search = list(range(job_config["options"]["jobs_submitted"]))

        parser = ExpParser()
        env = {"parameters": job_config["steering"]["parameters"]}

        def get_path_from_url(f_data: _OutFileData) -> str:
            url = cast(str, parser.parse(f_data["url"], job_config, env))
            if "//" not in url:
                path = url
            else:
                path = "/" + url.split("//", 1)[1].split("/", 1)[1]
            logging.info(f"checking path {path}")
            return path

        # search each possible file/task
        possible_outfiles = _IceProdV2Querier._get_outfiles(job_config)
        for f_data in reversed(possible_outfiles):
            for job in job_search:
                for i in range(f_data["iters"]):
                    logging.info(
                        f"Searching task: {f_data['task']}, job: {job}, iter: {i}"
                    )
                    if get_path_from_url(f_data) == filepath:
                        logging.info(f"Success on job: {job}, iter: {i}")
                        job_config["options"].update(
                            {"task": f_data["task"], "job": job, "iter": i}
                        )
                        return

        # if there's no match, at least assign the job_index
        if job_index:
            job_config["options"]["job"] = job_index
        raise OutFileNotFound()


# --------------------------------------------------------------------------------------
# Public Query-Manager interface functions


def _get_iceprod_querier(
    dataset_num: int,
    iceprodv2_rc: RestClient,
    iceprodv1_db: pymysql.connections.Connection,
) -> _IceProdQuerier:
    if dataset_num in _ICEPROD_V1_DATASET_RANGE:
        return _IceProdV1Querier(dataset_num, iceprodv1_db)
    if dataset_num in _ICEPROD_V2_DATASET_RANGE:
        return _IceProdV2Querier(dataset_num, iceprodv2_rc)
    raise DatasetNotFound(f"Dataset Num ({dataset_num}) is undefined.")


def _parse_dataset_num(filepath: str) -> int:
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


async def get_job_config(
    dataset_num: Optional[int],
    filepath: str,
    job_index: Optional[int],
    iceprodv2_rc: RestClient,
    iceprodv1_db: pymysql.connections.Connection,
) -> Tuple[dataclasses.Job, int]:
    """Get the job's config dict."""
    if dataset_num is not None:
        try:
            querier = _get_iceprod_querier(dataset_num, iceprodv2_rc, iceprodv1_db)
        except DatasetNotFound:
            dataset_num = None

    # if given dataset_num doesn't work (or was None), try parsing one from filepath
    if dataset_num is None:
        dataset_num = _parse_dataset_num(filepath)
        querier = _get_iceprod_querier(dataset_num, iceprodv2_rc, iceprodv1_db)

    job_config = await querier.get_job_config(filepath, job_index)

    return job_config, dataset_num


def grab_metadata(job_config: dataclasses.Job) -> types.IceProdMetadata:
    """Return the IceProdMetadata via `job_config`."""
    metadata: types.IceProdMetadata = {
        "dataset": job_config["options"]["dataset"],  # int
        "dataset_id": job_config["options"]["dataset_id"],  # str
        "job": job_config["options"].get("job"),  # int
    }

    # task data
    if "task_info" in job_config["options"]:
        metadata.update(
            {
                "job_id": job_config["options"]["task_info"].get("job_id"),  # str
                "task": job_config["options"]["task_info"].get("task"),  # str
                "task_id": job_config["options"]["task_info"].get("task_id"),  # str
            }
        )

    # config
    if job_config["options"]["dataset"] in _ICEPROD_V1_DATASET_RANGE:
        config_url = f'https://grid.icecube.wisc.edu/simulation/dataset/{job_config["options"]["dataset_id"]}'
    elif job_config["options"]["dataset"] in _ICEPROD_V2_DATASET_RANGE:
        config_url = f'https://iceprod2.icecube.wisc.edu/config?dataset_id={job_config["options"]["dataset_id"]}'
    else:
        raise DatasetNotFound()
    metadata["config"] = config_url  # str

    # del any Nones
    for key, val in list(metadata.items()):
        if val is None:
            del metadata[key]  # type: ignore[misc]

    return metadata


def grab_steering_parameters(job_config: dataclasses.Job) -> SteeringParameters:
    """Return the steering parameters dict."""
    params = job_config["steering"]["parameters"]

    return cast(SteeringParameters, params)
