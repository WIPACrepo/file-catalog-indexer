"""Classes for collecting metadata, on various types of files."""


import collections
import hashlib
import logging
import os
import re
import tarfile
import typing
import xml
import zlib
from datetime import date
from enum import Enum
from typing import Any, cast, Dict, Final, List, Optional, Tuple

import xmltodict  # type: ignore[import]

from ..utils import types
from . import filename_patterns

# Utility-Classes ----------------------------------------------------------------------


class FileInfo:  # pylint: disable=R0903
    """Wrapper around common file information.

    Similar to os.DirEntry.
    """

    def __init__(self, filepath: str):
        self.path = filepath
        self.name = os.path.basename(self.path)
        self.stat = lambda: os.stat(self.path)


class IceCubeSeason:
    """Wrapper static class encapsulating season-name - season-year mapping logic."""

    SEASONS: Dict[int, str] = {
        2005: "ICstring9",
        2006: "IC9",
        2007: "IC22",
        2008: "IC40",
        2009: "IC59",
        2010: "IC79",
        2011: "IC86-1",
        2012: "IC86-2",
        2013: "IC86-3",
        2014: "IC86-4",
        2015: "IC86-5",
        2016: "IC86-6",
        2017: "IC86-7",
        2018: "IC86-8",
        2019: "IC86-9",
        2020: "IC86-10",
    }

    @staticmethod
    def name_to_year(name: Optional[str]) -> Optional[int]:
        """Return the year of the season start for the season's `name`."""
        if not name:
            return None
        for season_year, season_name in IceCubeSeason.SEASONS.items():
            if season_name == name:
                return int(season_year)
        raise Exception(f"No season found for {name}.")

    @staticmethod
    def year_to_name(season_year: Optional[int]) -> Optional[str]:
        """Return the season's name for the year of the season start."""
        if not season_year:
            return None
        try:
            return IceCubeSeason.SEASONS[season_year]
        except KeyError:
            raise Exception(f"No season found for {season_year}.")


class ProcessingLevel(Enum):
    """Enum for processing-level constants."""

    PFRaw = "PFRaw"
    PFFilt = "PFFilt"
    PFDST = "PFDST"
    L2 = "L2"
    L3 = "L3"
    L4 = "L4"


# Metadata Classes ---------------------------------------------------------------------


class BasicFileMetadata:
    """The bare minimum metadata for a file.

    The metadata collected is a subset of the 'Core Metadata' documented
    in the schema: https://docs.google.com/document/d/14SanUWiYEbgarElt0YXSn_2We-rwT-ePO5Fg7rrM9lw/
    """

    def __init__(self, file: FileInfo, site: str):
        self.file = file
        self.site = site

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata: types.Metadata = {}
        metadata["logical_name"] = self.file.path
        metadata["checksum"] = {"sha512": self.sha512sum()}
        metadata["file_size"] = cast(int, self.file.stat().st_size)  # type: ignore[no-untyped-call]
        metadata["locations"] = [{"site": self.site, "path": self.file.path}]
        iso_date = date.fromtimestamp(os.path.getctime(self.file.path)).isoformat()
        metadata["create_date"] = iso_date
        return metadata

    def sha512sum(self) -> str:
        """Return the SHA512 checksum of the file given by path."""
        bufsize = 4194304
        sha = hashlib.new("sha512")
        with open(self.file.path, "rb", buffering=0) as file:
            line = file.read(bufsize)
            while line:
                sha.update(line)
                line = file.read(bufsize)
        return sha.hexdigest()


class I3FileMetadata(BasicFileMetadata):
    """Metadata for i3 files."""

    def __init__(
        self,
        file: FileInfo,
        site: str,
        processing_level: ProcessingLevel,
        filename_patterns_: List[str],
    ):
        super().__init__(file, site)
        self.processing_level = processing_level
        self.meta_xml: Dict[str, Any] = {}
        try:
            (
                self.season_year,
                self.run,
                self.subrun,
                self.part,
            ) = I3FileMetadata.parse_year_run_subrun_part(
                filename_patterns_, self.file.name
            )
        except ValueError:
            raise Exception(
                f"Filename not in a known {self.processing_level.value} file format, {file.name}."
            )

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata = super().generate()

        start_dt, end_dt, create_date, software = self._parse_xml()
        data_type = self._get_data_type()
        first_event, last_event, event_count, status = self._get_events_data()

        metadata["create_date"] = create_date  # Override BasicFileMetadata's value
        metadata["data_type"] = data_type
        metadata["processing_level"] = self.processing_level.value
        metadata["content_status"] = status
        metadata["software"] = software

        if data_type == "real":
            metadata["run"] = {
                "run_number": self.run,
                "subrun_number": self.subrun,
                "part_number": self.part,
                "start_datetime": start_dt,
                "end_datetime": end_dt,
                "first_event": first_event,
                "last_event": last_event,
                "event_count": event_count,
            }
        return metadata

    @staticmethod
    def parse_year_run_subrun_part(
        patterns: List[str], filename: str
    ) -> Tuple[Optional[int], int, int, int]:
        r"""Return the year, run, subrun, and part by parsing the `filename` according to regex `patterns`.

        Uses named groups: `year`, `run`, `subrun`, and `part`.
        - Only a `run` group is required in the filename/regex pattern.
        - Optionally include `ic_strings` group (\d+), instead of `year` group.
        """
        for p in patterns:
            if "?P<run>" not in p:
                raise Exception(f"Pattern does not have `run` regex group, {p}.")

            match = re.match(p, filename)
            if match:
                values = match.groupdict()
                # get year
                if "ic_strings" in values:
                    year = IceCubeSeason.name_to_year(f"IC{values['ic_strings']}")
                else:
                    try:
                        year = int(values["year"])
                    except KeyError:
                        year = None
                # get run
                try:
                    run = int(values["run"])
                except KeyError:
                    run = 0
                # get subrun
                try:
                    subrun = int(values["subrun"])
                except KeyError:
                    subrun = 0
                # get part
                try:
                    part = int(values["part"])
                except KeyError:
                    part = 0

                return year, run, subrun, part

        # fall-through
        raise ValueError(f"Filename does not match any pattern, {filename}.")

    @staticmethod
    def parse_run_number(filename: str) -> int:
        """Return run number from `filename`."""
        # Ex: Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        # Ex: Run00125791_GapsTxt.tar
        match = re.match(r".*Run(?P<run>\d+)", filename)
        try:
            run = match.groupdict()["run"]  # type: ignore[union-attr]
            return int(run)
        except Exception:
            raise Exception(f"No run number found in filename, {filename}.")

    def _get_data_type(self) -> Optional[str]:
        """Return the file data type, real or simulation."""
        if "/exp/" in self.file.path:
            return "real"
        if "/sim/" in self.file.path:
            return "simulation"
        return None

    def _parse_xml(
        self,
    ) -> Tuple[Optional[str], Optional[str], str, Optional[List[types.SoftwareEntry]]]:
        """Return data points from `self.meta_xml` dict."""
        start_dt = None
        end_dt = None
        create_date = None
        software = None

        if self.meta_xml:
            try:
                start_dt = str(self.meta_xml["DIF_Plus"]["Plus"]["Start_DateTime"])
            except KeyError:
                pass
            try:
                end_dt = str(self.meta_xml["DIF_Plus"]["Plus"]["End_DateTime"])
            except KeyError:
                pass
            try:
                create_date = str(self.meta_xml["DIF_Plus"]["DIF"]["DIF_Creation_Date"])
            except KeyError:
                pass
            try:
                software = self._get_software()
            except KeyError:
                pass

        if not create_date:
            ctime = os.path.getctime(self.file.path)
            create_date = date.fromtimestamp(ctime).isoformat()

        return start_dt, end_dt, create_date, software

    def _get_software(self) -> List[types.SoftwareEntry]:
        """Return software metadata from `self.meta_xml`."""

        def parse_project(project: Dict[str, Any]) -> types.SoftwareEntry:
            software: types.SoftwareEntry = {}
            if "Name" in project:
                software["name"] = str(project["Name"])
            if "Version" in project:
                software["version"] = str(project["Version"])
            if "DateTime" in project:
                software["date"] = str(project["DateTime"])
            return software

        software_list = []
        entry = self.meta_xml["DIF_Plus"]["Plus"]["Project"]
        entry_type = type(entry)

        if entry_type is list:
            for project in entry:
                software_list.append(parse_project(project))
        elif entry_type is collections.OrderedDict:
            software_list = [parse_project(entry)]
        else:
            raise Exception(
                f"meta xml file has unanticipated 'Project' type {entry_type}."
            )

        return software_list

    def _get_events_data(self) -> Tuple[Optional[int], Optional[int], int, str]:
        """Return events data as a tuple.

        AKA: the first event id, last event id, number of events, and content
        status.
        """
        first = float("inf")
        last = float("-inf")
        count = 0
        status = "good"

        from icecube import dataio  # type: ignore[import] # pylint: disable=C0415,E0401

        try:
            for frame in dataio.I3File(self.file.path):
                if "I3EventHeader" in frame:
                    count = count + 1
                    event_id = int(frame["I3EventHeader"].event_id)
                    # check if event_id precedes `first`
                    if first > event_id:
                        first = event_id
                    # check if event_id succeeds `last`
                    if last < event_id:
                        last = event_id
        except:  # noqa: E722  # pylint: disable=W0702
            status = "bad"

        return (
            None if first == float("inf") else typing.cast(int, first),
            None if last == float("-inf") else typing.cast(int, last),
            count,
            status,
        )

    def _grab_meta_xml_from_tar(self) -> None:
        """Get the meta-xml dict form the tar file.

        1. Untar `self.file.path` (in memory)
        2. Set the '*meta.xml' file as `self.meta_xml`.
        """
        try:
            with tarfile.open(self.file.path) as tar:
                for tar_obj in tar:
                    if ".meta.xml" in tar_obj.name:
                        self.meta_xml = xmltodict.parse(tar.extractfile(tar_obj))
        except (
            xml.parsers.expat.ExpatError,
            tarfile.ReadError,
            EOFError,
            zlib.error,
        ) as e:
            logging.info(
                f"Cannot get *meta.xml file from {self.file.path}, {e.__class__.__name__}."
            )


class L2FileMetadata(I3FileMetadata):
    """Metadata for L2 i3 files."""

    FILENAME_PATTERNS: Final[List[str]] = filename_patterns.L2["patterns"]

    def __init__(  # pylint: disable=R0913
        self,
        file: FileInfo,
        site: str,
        dir_meta_xml: Dict[str, Any],
        gaps_dict: Dict[str, Any],
        gcd_filepath: str,
    ):
        super().__init__(
            file, site, ProcessingLevel.L2, L2FileMetadata.FILENAME_PATTERNS
        )
        self.meta_xml = dir_meta_xml
        self.gaps_dict = gaps_dict
        self.gcd_filepath = gcd_filepath

    def _parse_gaps_dict(
        self,
    ) -> Tuple[
        Optional[List[types.GapEntry]],
        Optional[float],
        Optional[types.Event],
        Optional[types.Event],
    ]:
        """Return formatted data points from `self.gaps_dict`."""
        if not self.gaps_dict:
            return None, None, None, None

        livetime = float(self.gaps_dict["File Livetime"])  # Ex: 0.92
        if livetime < 0:  # corrupted value, don't read any more values
            return None, None, None, None

        from icecube import dataclasses  # pylint: disable=E0401,C0415

        try:
            # Ex: '53162019 2018 206130762188498'
            first = self.gaps_dict["First Event of File"].split()

            # Ex: '53164679 2018 206139955965204'
            last = self.gaps_dict["Last Event of File"].split()

            first_id = int(first[0])
            first_dt = dataclasses.I3Time(int(first[1]), int(first[2])).date_time

            last_id = int(last[0])
            last_dt = dataclasses.I3Time(int(last[1]), int(last[2])).date_time

            gaps: List[types.GapEntry] = [
                {
                    "start_event_id": first_id,
                    "stop_event_id": last_id,
                    "delta_time": (last_dt - first_dt).total_seconds(),
                    "start_date": first_dt.isoformat(),
                    "stop_date": last_dt.isoformat(),
                }
            ]

            first_event_dict: types.Event = {
                "event_id": first_id,
                "datetime": first_dt.isoformat(),
            }
            last_event_dict: types.Event = {
                "event_id": last_id,
                "datetime": last_dt.isoformat(),
            }

            return gaps, livetime, first_event_dict, last_event_dict

        except KeyError:
            return None, livetime, None, None

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata = super().generate()
        gaps, livetime, first_event_dict, last_event_dict = self._parse_gaps_dict()
        metadata["offline_processing_metadata"] = {
            # 'dataset_id': None,
            "season": self.season_year,
            "season_name": IceCubeSeason.year_to_name(self.season_year),
            "L2_gcd_file": self.gcd_filepath,
            # 'L2_snapshot_id': None,
            # 'L2_production_version': None,
            # 'L3_source_dataset_id': None,
            # 'working_group': None,
            # 'validation_validated': None,
            # 'validation_date': None,
            # 'validation_software': {},
            "livetime": livetime,
            "gaps": gaps,
            "first_event": first_event_dict,
            "last_event": last_event_dict,
        }
        return metadata

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid L2 filename.

        Check if `filename` matches the base filename pattern for L2
        files.
        """
        return bool(re.match(filename_patterns.L2["base_pattern"], filename))


class PFFiltFileMetadata(I3FileMetadata):
    """Metadata for PFFilt i3 files."""

    FILENAME_PATTERNS: Final[List[str]] = filename_patterns.PFFilt["patterns"]

    def __init__(self, file: FileInfo, site: str):
        super().__init__(
            file, site, ProcessingLevel.PFFilt, PFFiltFileMetadata.FILENAME_PATTERNS
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFFilt filename.

        Check if `filename` matches the base filename pattern for PFFilt
        files.
        """
        return bool(re.match(filename_patterns.PFFilt["base_pattern"], filename))


class PFDSTFileMetadata(I3FileMetadata):
    """Metadata for PFDST i3 files."""

    FILENAME_PATTERNS: Final[List[str]] = filename_patterns.PFDST["patterns"]

    def __init__(self, file: FileInfo, site: str):
        super().__init__(
            file, site, ProcessingLevel.PFDST, PFDSTFileMetadata.FILENAME_PATTERNS
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFDST filename.

        Check if `filename` matches the base filename pattern for PFDST
        files.
        """
        return bool(re.match(filename_patterns.PFDST["base_pattern"], filename))


class PFRawFileMetadata(I3FileMetadata):
    """Metadata for PFRaw i3 files."""

    FILENAME_PATTERNS: Final[List[str]] = filename_patterns.PFRaw["patterns"]

    def __init__(self, file: FileInfo, site: str):
        super().__init__(
            file, site, ProcessingLevel.PFRaw, PFRawFileMetadata.FILENAME_PATTERNS
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFRaw filename.

        Check if `filename` matches the base filename pattern for PFRaw
        files.
        """
        return bool(re.match(filename_patterns.PFRaw["base_pattern"], filename))
