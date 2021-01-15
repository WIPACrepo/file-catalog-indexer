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

from ...utils import types

_ICEPROD_V2_DATASET_RANGE = range(20000, 30000)
_ICEPROD_V1_DATASET_RANGE = range(0, 20000)


class _OutFileData(TypedDict):
    url: str
    iters: int
    task: str


class DatasetNotFound(Exception):
    """Raise when an IceProd dataset cannot be found."""


class TaskNotFound(Exception):
    """Raise when an IceProd task cannot be found."""


class OutFileNotFound(Exception):
    """Raise when an IceProd outfile cannot be found."""


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

    async def get_job_config(
        self, filepath: str, job_index: Optional[int]
    ) -> dataclasses.Job:
        """Get the job's config dict, AKA `dataclasses.Job`."""
        raise NotImplementedError()


class _IceProdV1Querier(_IceProdQuerier):
    """Manage IceProd v1 queries."""

    async def get_job_config(
        self, filepath: str, job_index: Optional[int]
    ) -> dataclasses.Job:
        pass
        # TODO


class _IceProdV2Querier(_IceProdQuerier):
    """Manage IceProd v2 queries."""

    async def _get_dataset_info(self) -> Tuple[str, int]:
        datasets = await self.rest_client.request(
            "GET", "/datasets?keys=dataset_id|dataset|jobs_submitted"
        )  # TODO -- offload this to further up call stack

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

        ret = await self.rest_client.request("GET", f"/config/{dataset_id}")
        job_config = dict_to_dataclasses(ret)

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
                ] in ("output", "both",):
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

        ret = await self.rest_client.request(
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


def _get_iceprod_querier(dataset_num: int, iceprodv2_rc: RestClient) -> _IceProdQuerier:
    if dataset_num in _ICEPROD_V1_DATASET_RANGE:
        return _IceProdV1Querier(dataset_num, None)  # TODO
    if dataset_num in _ICEPROD_V2_DATASET_RANGE:
        return _IceProdV2Querier(dataset_num, iceprodv2_rc)
    raise DatasetNotFound(f"Dataset Num ({dataset_num}) is undefined.")


async def get_job_config(
    dataset_num: int, filepath: str, job_index: Optional[int], iceprodv2_rc: RestClient
) -> Tuple[dataclasses.Job, int]:
    """Get the job's config dict."""
    try:
        ip_querier = _get_iceprod_querier(dataset_num, iceprodv2_rc)
    except DatasetNotFound:  # if given dataset num doesn't work try parsing from filepath
        dataset_num = parse_dataset_num(filepath)
        ip_querier = _get_iceprod_querier(dataset_num, iceprodv2_rc)

    job_config = await ip_querier.get_job_config(filepath, job_index)

    return job_config, dataset_num


def grab_metadata(job_config: dataclasses.Job) -> types.IceProdMetadata:
    """Return the IceProdMetadata via `job_config`."""
    # TODO - IceProd 1
    config_url = f'https://iceprod2.icecube.wisc.edu/config?dataset_id={job_config["options"]["dataset_id"]}'

    metadata: types.IceProdMetadata = {
        "dataset": job_config["options"]["dataset"],  # int
        "dataset_id": job_config["options"]["dataset_id"],  # str
        "job": job_config["options"]["job"],  # int
        "job_id": job_config["options"]["task_info"]["job_id"],  # str
        "task": job_config["options"]["task_info"]["name"],  # str
        "task_id": job_config["options"]["task_info"]["task_id"],  # str
        "config": config_url,  # str
    }

    return metadata


def grab_steering_paramters(
    job_config: dataclasses.Job,
) -> Dict[str, Union[str, float, int]]:
    """Return the steering parameters dict with macros resolved."""
    # TODO - IceProd 1?
    params = ExpParser().parse(
        job_config["steering"]["parameters"],
        job_config,
        {"parameters": job_config["steering"]["parameters"]},
    )

    return cast(Dict[str, Union[str, float, int]], params)
