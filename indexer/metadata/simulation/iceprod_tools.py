"""Interface with IceProd REST interface.

Based on https://github.com/WIPACrepo/iceprod/blob/master/resources/get_file_info.py.
"""

# pylint: disable=R0903

import functools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union, cast

import pymysql
from file_catalog.schema import types
from iceprod.core import dataclasses  # type: ignore[import]
from iceprod.core.parser import ExpParser  # type: ignore[import]
from iceprod.core.serialization import dict_to_dataclasses  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

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


class _IP2RESTDatasetTask(TypedDict):
    name: str
    task_id: str
    task: str
    job_id: str


class DatasetNotFound(Exception):
    """Raise when an IceProd dataset cannot be found."""


class TaskNotFound(Exception):
    """Raise when an IceProd task cannot be found."""


# --------------------------------------------------------------------------------------
# IceProd Connection interface


class IceProdConnection:
    """Interface for connecting to IceProd v1 and v2."""

    def __init__(self, iceprodv1_pass: str, iceprodv2_token: str):
        if not iceprodv1_pass:
            raise RuntimeError("Missing IceProd v1 DB password")
        elif not iceprodv2_token:
            raise RuntimeError("Missing IceProd v2 REST token")

        self._iceprodv1_pass = iceprodv1_pass
        self._iceprodv2_rc = RestClient(
            "https://iceprod2-api.icecube.wisc.edu", iceprodv2_token
        )

    def get_iceprodv1_db(self) -> pymysql.connections.Connection:
        """Get a pymsql connection instance for querying the IceProd v1 DB."""
        return pymysql.connect(
            host="vm-i3simprod.icecube.wisc.edu",
            user="i3simprod-ro",
            passwd=self._iceprodv1_pass,
            db="i3simprod",
        )

    def get_iceprodv2_rc(self) -> RestClient:
        """Get a REST client instance for querying the IceProd v2 DB."""
        return self._iceprodv2_rc


# --------------------------------------------------------------------------------------
# Private Query Managers


class _IceProdQuerier:
    """Manage IceProd queries."""

    def __init__(
        self, dataset_num: int, iceprod_conn: IceProdConnection, filepath: str
    ):
        self.iceprod_conn = iceprod_conn
        self.dataset_num = dataset_num
        self._filepath = filepath

    @property
    def filepath(self) -> str:  # pylint: disable=C0116
        return self._filepath

    def get_steering_params_and_ip_metadata(
        self, job_index: Optional[int]
    ) -> Tuple[SteeringParameters, types.IceProdMetadata]:
        """Get the job's config dict, AKA `dataclasses.Job`."""
        raise NotImplementedError()

    @staticmethod
    def _expand_steering_parameters(job_config: dataclasses.Job) -> SteeringParameters:
        job_config["steering"]["parameters"] = ExpParser().parse(
            job_config["steering"]["parameters"],
            job_config,
            {"parameters": job_config["steering"]["parameters"]},
        )
        return cast(SteeringParameters, job_config["steering"]["parameters"])


# --------------------------------------------------------------------------------------
# IceProd v1


@functools.lru_cache()
def _get_iceprod1_dataset_steering_params(
    iceprod_conn: IceProdConnection, dataset_num: int
) -> List[Dict[str, Any]]:
    logging.debug(
        f"No cache hit for dataset_num={dataset_num}. Querying IceProd1 DB..."
    )

    sql = (
        "SELECT * FROM steering_parameter "
        f"WHERE dataset_id = {dataset_num} "
        "ORDER by name"
    )

    conn = iceprod_conn.get_iceprodv1_db()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    cursor.execute(sql)
    results: List[Dict[str, Any]] = cursor.fetchall()  # type: ignore[assignment]
    cursor.close()
    conn.close()

    return results


class _IceProdV1Querier(_IceProdQuerier):
    """Manage IceProd v1 queries."""

    def _query_steering_params(self) -> SteeringParameters:
        steering_params = {}

        logging.debug(f"Grabbing steering parameters ({self.filepath})...")
        results = _get_iceprod1_dataset_steering_params(
            self.iceprod_conn, self.dataset_num
        )

        if not results:
            raise DatasetNotFound()

        for param in results:
            value = param["value"]
            for html_tag in _HTML_TAGS:
                value = value.replace(html_tag, "")
            steering_params[param["name"]] = value

        return steering_params

    def get_steering_params_and_ip_metadata(
        self, job_index: Optional[int]
    ) -> Tuple[SteeringParameters, types.IceProdMetadata]:

        i3_metadata: types.IceProdMetadata = {
            "dataset": self.dataset_num,
            "dataset_id": str(self.dataset_num),
            "job": job_index,
            "job_id": str(job_index) if job_index is not None else None,
            "task": None,
            "task_id": None,
            "config": f"https://grid.icecube.wisc.edu/simulation/dataset/{self.dataset_num}",
        }

        steering_params = self._expand_steering_parameters(
            dict_to_dataclasses(
                {
                    "steering": {"parameters": self._query_steering_params()},
                    "options": i3_metadata,
                }
            )
        )

        return steering_params, i3_metadata


# --------------------------------------------------------------------------------------
# IceProd v2


@functools.lru_cache()
def _get_all_iceprod2_datasets(
    iceprod_conn: IceProdConnection,
) -> Dict[int, _IP2RESTDataset]:
    """Return dict of datasets keyed by their dataset num."""
    logging.debug("No cache hit for all datasets. Requesting IceProd2...")

    datasets = iceprod_conn.get_iceprodv2_rc().request_seq(
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
def _get_iceprod2_dataset_job_config(
    iceprod_conn: IceProdConnection, dataset_id: str
) -> dataclasses.Job:
    logging.debug(f"No cache hit for dataset_id={dataset_id}. Requesting IceProd2...")

    ret = iceprod_conn.get_iceprodv2_rc().request_seq("GET", f"/config/{dataset_id}")
    job_config = dict_to_dataclasses(ret)

    if not job_config["steering"]:  # dataclasses.Job sets "steering" to None by default
        job_config["steering"] = {}
    if not job_config["steering"].get("parameters", None):
        job_config["steering"]["parameters"] = {}

    return job_config


@functools.lru_cache()
def _get_iceprod2_dataset_tasks(
    iceprod_conn: IceProdConnection, dataset_id: str, job_index: int
) -> Dict[str, _IP2RESTDatasetTask]:
    logging.debug(
        f"No cache hit for dataset_id={dataset_id}, job_index={job_index}. "
        "Requesting IceProd2..."
    )

    ret = iceprod_conn.get_iceprodv2_rc().request_seq(
        "GET",
        f"/datasets/{dataset_id}/tasks",
        {"job_index": job_index, "keys": "name|task_id|job_id|task_index"},
    )

    task_dicts: Dict[str, _IP2RESTDatasetTask] = {t["name"]: t for t in ret.values()}

    return task_dicts


class _IceProdV2Querier(_IceProdQuerier):
    """Manage IceProd v2 queries."""

    def _get_dataset_info(self) -> Tuple[str, int]:
        logging.debug(f"Grabbing dataset info ({self.filepath})...")
        datasets = _get_all_iceprod2_datasets(self.iceprod_conn)
        try:
            dataset_id = datasets[self.dataset_num]["dataset_id"]
            jobs_submitted = datasets[self.dataset_num]["jobs_submitted"]
        except KeyError:
            raise DatasetNotFound(f"dataset num {self.dataset_num} not found")

        return dataset_id, jobs_submitted

    def get_steering_params_and_ip_metadata(
        self, job_index: Optional[int]
    ) -> Tuple[SteeringParameters, types.IceProdMetadata]:
        dataset_id, jobs_submitted = self._get_dataset_info()

        job_index, task_name, steering_params = self._get_outfile_info(
            dataset_id, job_index, jobs_submitted
        )

        try:
            task_id, job_id = None, None
            task_id, job_id = self._get_task_info(dataset_id, job_index, task_name)
        except TaskNotFound:
            logging.warning(
                f"Could not get task info ({self.filepath}): "
                f"dataset_id={dataset_id}, job_index={job_index}, task_name={task_name}"
            )

        i3_metadata: types.IceProdMetadata = {
            "dataset": self.dataset_num,
            "dataset_id": dataset_id,
            "job": job_index,
            "job_id": job_id,
            "task": task_name,
            "task_id": task_id,
            "config": f"https://iceprod2.icecube.wisc.edu/config?dataset_id={dataset_id}",
        }

        return steering_params, i3_metadata

    @staticmethod
    def _get_outfiles(job_config: dataclasses.Job) -> List[_OutFileData]:
        """Get every single outputted file, plus some data on each."""

        def do_append(datum: Dict[str, str]) -> bool:
            ok_types = ("permanent", "site_temp")
            ok_movements = ("output", "both")
            return datum["type"] in ok_types and datum["movement"] in ok_movements

        files: List[_OutFileData] = []
        # Search each task's data
        for task in job_config["tasks"]:
            for task_d in task["data"]:
                if do_append(task_d):
                    files.append(
                        {"url": task_d["remote"], "iters": 1, "task": task["name"]}
                    )
            # Search each tray's data
            for tray in task["trays"]:
                for tray_d in tray["data"]:
                    if do_append(tray_d):
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
                        if do_append(module_d):
                            files.append(
                                {
                                    "url": module_d["remote"],
                                    "iters": tray["iterations"],
                                    "task": task["name"],
                                }
                            )

        return files

    def _get_task_info(
        self, dataset_id: str, job_index: Optional[int], task_name: Optional[str]
    ) -> Tuple[str, str]:
        if job_index is None or task_name is None:
            raise TaskNotFound()

        logging.debug(f"Grabbing task info ({self.filepath})...")
        task_dicts = _get_iceprod2_dataset_tasks(
            self.iceprod_conn, dataset_id, job_index
        )

        try:
            return (
                task_dicts[task_name]["task_id"],
                task_dicts[task_name]["job_id"],
            )
        except KeyError:
            raise TaskNotFound()

    def _get_outfile_info(
        self, dataset_id: str, job_index: Optional[int], jobs_submitted: int,
    ) -> Tuple[Optional[int], Optional[str], SteeringParameters]:
        """Get `task`, `job`, and steering parameters."""
        if job_index is not None:  # do we already know what job to look at?
            job_search: List[int] = [job_index]
        else:  # otherwise, look at each job from dataset
            job_search = list(range(jobs_submitted))

        logging.debug(f"Grabbing dataset job config ({self.filepath})...")
        job_config = _get_iceprod2_dataset_job_config(self.iceprod_conn, dataset_id,)
        job_config["options"].update(
            {
                "dataset": self.dataset_num,
                "dataset_id": dataset_id,
                "jobs_submitted": jobs_submitted,
            }
        )

        parser = ExpParser()
        env = {"parameters": job_config["steering"]["parameters"]}

        def is_filepath_match(f_data: _OutFileData) -> bool:
            url = cast(str, parser.parse(f_data["url"], job_config, env))
            if "//" not in url:
                path = url
            else:
                path = "/" + url.split("//", 1)[1].split("/", 1)[1]
            logging.debug(
                f"Looking for outfile ({self.filepath}) "
                f"({job_config['options']} -> {path})..."
            )
            return path == self.filepath

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
                    if is_filepath_match(f_data):
                        return (
                            job_config["options"]["job"],
                            job_config["options"]["task"],
                            self._expand_steering_parameters(job_config),
                        )

        # cleanup & raise
        logging.warning(f"Outfile ({self.filepath}) could not be matched.")
        job_config["options"].pop("task", None)
        job_config["options"].pop("job", None)
        job_config["options"].pop("iter", None)
        if job_index is not None:  # if there's no match, at least assign the job_index
            job_config["options"]["job"] = job_index

        return (
            job_config["options"].get("job"),  # might be None
            None,
            self._expand_steering_parameters(job_config),
        )


# --------------------------------------------------------------------------------------
# Public Query-Manager interface functions


def _get_iceprod_querier(
    dataset_num: int, iceprod_conn: IceProdConnection, filepath: str,
) -> _IceProdQuerier:
    if dataset_num in _ICEPROD_V1_DATASET_RANGE:
        return _IceProdV1Querier(dataset_num, iceprod_conn, filepath)
    elif dataset_num in _ICEPROD_V2_DATASET_RANGE:
        return _IceProdV2Querier(dataset_num, iceprod_conn, filepath)
    else:
        raise DatasetNotFound(f"Dataset Num ({dataset_num}) is undefined.")


def _parse_dataset_num_from_dirpath(filepath: str) -> int:
    """Return the dataset num by parsing the directory path."""
    # try IP2 first: IP1 uses smaller numbers, so false-positive matches are more likely
    for dataset_range in [_ICEPROD_V2_DATASET_RANGE, _ICEPROD_V1_DATASET_RANGE]:
        parts = filepath.split("/")
        for dir_ in reversed(parts[:-1]):  # ignore the filename; search right-to-left
            try:
                dataset_num = int(dir_)
                if dataset_num in dataset_range:
                    return dataset_num
            except ValueError:
                continue
    raise DatasetNotFound(f"Could not determine dataset number: {filepath}")


def get_steering_params_and_ip_metadata(
    dataset_num: Optional[int],
    filepath: str,
    job_index: Optional[int],
    iceprod_conn: IceProdConnection,
) -> Tuple[SteeringParameters, types.IceProdMetadata]:
    """Get the dataset's steering parameters and `IceProdMetadata`."""
    if dataset_num is not None:
        try:
            querier = _get_iceprod_querier(dataset_num, iceprod_conn, filepath)
        except DatasetNotFound:
            dataset_num = None

    # if given dataset_num doesn't work (or was None), try parsing one from filepath
    if dataset_num is None:
        dataset_num = _parse_dataset_num_from_dirpath(filepath)
        querier = _get_iceprod_querier(dataset_num, iceprod_conn, filepath)

    steering_params, ip_metadata = querier.get_steering_params_and_ip_metadata(
        job_index
    )

    return steering_params, ip_metadata
