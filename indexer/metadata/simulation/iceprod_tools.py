"""Interface with IceProd REST interface.

Based on https://github.com/WIPACrepo/iceprod/blob/master/resources/get_file_info.py.
"""

# pylint: disable=R0903

import functools
import logging  # TODO - trim down
from typing import Any, cast, Dict, List, Optional, Tuple, Union

import pymysql

# local imports
from iceprod.core import dataclasses  # type: ignore[import]
from iceprod.core.parser import ExpParser  # type: ignore[import]
from iceprod.core.serialization import dict_to_dataclasses  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

from ...utils import types

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


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


# --------------------------------------------------------------------------------------
# IceProd v1


@functools.lru_cache()
def _get_iceprod1_dataset_steering_params(
    iceprodv1_db: pymysql.connections.Connection, dataset_num: int
) -> List[Dict[str, Any]]:
    sql = (
        "SELECT * FROM steering_parameter "
        f"WHERE dataset_id = {dataset_num} "
        "ORDER by name"
    )

    cursor = iceprodv1_db.cursor(pymysql.cursors.DictCursor)
    cursor.execute(sql)
    results: List[Dict[str, Any]] = cursor.fetchall()  # type: ignore[assignment]

    return results


class _IceProdV1Querier(_IceProdQuerier):
    """Manage IceProd v1 queries."""

    def __init__(self, dataset_num: int, iceprodv1_db: pymysql.connections.Connection):
        super().__init__(dataset_num)
        self.iceprodv1_db = iceprodv1_db

    def _query_steering_params(self) -> SteeringParameters:
        steering_params = {}

        results = _get_iceprod1_dataset_steering_params(
            self.iceprodv1_db, self.dataset_num
        )

        if not results:
            raise DatasetNotFound()

        for param in results:
            value = param["value"]
            for html_tag in _HTML_TAGS:
                value = value.replace(html_tag, "")
            steering_params[param["name"]] = value

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


# --------------------------------------------------------------------------------------
# IceProd v2


@functools.lru_cache()
async def _get_all_iceprod2_datasets(
    iceprodv2_rc: RestClient,
) -> Dict[int, _IP2RESTDataset]:
    """Return dict of datasets keyed by their dataset num."""
    datasets = await iceprodv2_rc.request(
        "GET", "/datasets?keys=dataset_id|dataset|jobs_submitted"
    )

    ret: Dict[int, _IP2RESTDataset] = {}
    for info in datasets.values():
        ret[int(info["dataset"])] = {
            "dataset_id": info["dataset_id"],
            "jobs_submitted": info["jobs_submitted"],
        }

    return ret


@functools.lru_cache()
async def _get_iceprod2_dataset_job_config(
    iceprodv2_rc: RestClient, dataset_id: str
) -> dataclasses.Job:
    ret = await iceprodv2_rc.request("GET", f"/config/{dataset_id}")
    job_config = dict_to_dataclasses(ret)

    return job_config


@functools.lru_cache()
async def _get_iceprod2_dataset_tasks(
    iceprodv2_rc: RestClient, dataset_id: str, job_index: int
) -> Dict[str, Any]:
    ret = iceprodv2_rc.request(
        "GET",
        f"/datasets/{dataset_id}/tasks",
        {"job_index": job_index, "keys": "name|task_id|job_id|task_index"},
    )

    return cast(Dict[str, Any], ret)


class _IceProdV2Querier(_IceProdQuerier):
    """Manage IceProd v2 queries."""

    def __init__(self, dataset_num: int, iceprodv2_rc: RestClient):
        super().__init__(dataset_num)
        self.iceprodv2_rc = iceprodv2_rc

    async def _get_dataset_info(self) -> Tuple[str, int]:
        datasets = await _get_all_iceprod2_datasets(self.iceprodv2_rc)
        try:
            return (
                datasets[self.dataset_num]["dataset_id"],
                datasets[self.dataset_num]["jobs_submitted"],
            )
        except KeyError:
            raise DatasetNotFound(f"dataset num {self.dataset_num} not found")

    async def get_job_config(
        self, filepath: str, job_index: Optional[int]
    ) -> dataclasses.Job:
        dataset_id, jobs_submitted = await self._get_dataset_info()

        job_config: dataclasses.Job = await _get_iceprod2_dataset_job_config(
            self.iceprodv2_rc, dataset_id
        )

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

        ret = await _get_iceprod2_dataset_tasks(
            self.iceprodv2_rc,
            job_config["options"]["dataset_id"],
            job_config["options"]["job"],
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

        # search each possible file/task from job(s)/iters
        possible_outfiles = _IceProdV2Querier._get_outfiles(job_config)
        for f_data in reversed(possible_outfiles):
            job_config["options"]["task"] = f_data["task"]
            # job
            for job in job_search:
                job_config["options"]["job"] = job
                # iter
                for i in range(f_data["iters"]):
                    job_config["options"]["iter"] = i
                    logging.info(f'Searching: {job_config["options"]}')
                    if get_path_from_url(f_data) == filepath:
                        logging.info(f'Success on {job_config["options"]}')
                        return

        # cleanup & raise
        job_config["options"].pop("task", None)
        job_config["options"].pop("job", None)
        job_config["options"].pop("iter", None)
        if job_index:  # if there's no match, at least assign the job_index
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
    elif dataset_num in _ICEPROD_V2_DATASET_RANGE:
        return _IceProdV2Querier(dataset_num, iceprodv2_rc)
    else:
        raise DatasetNotFound(f"Dataset Num ({dataset_num}) is undefined.")


def _parse_dataset_num_from_dirpath(filepath: str) -> int:
    """Return the dataset num by parsing the directory path."""
    # try IP2 first: IP1 uses smaller numbers, so false-positive matches are more likely
    for dataset_range in [_ICEPROD_V2_DATASET_RANGE, _ICEPROD_V1_DATASET_RANGE]:
        parts = filepath.split("/")
        for p in reversed(parts[:-1]):  # ignore the filename; search right-to-left
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
        dataset_num = _parse_dataset_num_from_dirpath(filepath)
        querier = _get_iceprod_querier(dataset_num, iceprodv2_rc, iceprodv1_db)

    job_config = await querier.get_job_config(filepath, job_index)

    return job_config, dataset_num


def grab_iceprod_metadata(job_config: dataclasses.Job) -> types.IceProdMetadata:
    """Return the IceProdMetadata via `job_config`."""
    ip_metadata: types.IceProdMetadata = {
        "dataset": job_config["options"]["dataset"],  # int
        "dataset_id": job_config["options"]["dataset_id"],  # str
        "job": job_config["options"].get("job"),  # int
    }

    # task data
    if "task_info" in job_config["options"]:
        ip_metadata.update(
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
    ip_metadata["config"] = config_url  # str

    # del any Nones
    for key, val in list(ip_metadata.items()):
        if val is None:
            del ip_metadata[key]  # type: ignore[misc]

    return ip_metadata


def grab_steering_parameters(job_config: dataclasses.Job) -> SteeringParameters:
    """Return the steering parameters dict."""
    params = job_config["steering"]["parameters"]

    return cast(SteeringParameters, params)
