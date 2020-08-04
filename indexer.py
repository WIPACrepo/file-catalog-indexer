"""Data-indexing script for File Catalog."""

import argparse
import asyncio
import collections
import hashlib
import logging
import math
import os
import re
import stat
import string
import tarfile
import typing
import xml
import zlib
from concurrent.futures import Future, ProcessPoolExecutor
from datetime import date
from enum import Enum
from time import sleep
from typing import Any, Dict, List, Optional, Tuple, TypedDict

import requests
import xmltodict  # type: ignore[import]
import yaml
from icecube import dataclasses, dataio  # type: ignore[import]  # pylint: disable=E0401

# local imports
from rest_tools.client import RestClient  # type: ignore[import]

# types


class RestClientArgs(TypedDict):
    """TypedDict for RestClient parameters."""

    url: str
    token: str
    timeout: int
    retries: int


class IndexerFlags(TypedDict):
    """TypedDict for Indexer bool parameters."""

    basic_only: bool
    no_patch: bool


ACCEPTED_ROOTS = ["/data"]  # don't include trailing slash


def is_processable_path(path: str) -> bool:
    """Return `True` if `path` is processable.

    AKA, not a symlink, a socket, a FIFO, a device, nor char device.
    """
    mode = os.lstat(path).st_mode
    return not (
        stat.S_ISLNK(mode)
        or stat.S_ISSOCK(mode)
        or stat.S_ISFIFO(mode)
        or stat.S_ISBLK(mode)
        or stat.S_ISCHR(mode)
    )


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

    SEASONS = {
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
    }  # type: Dict[int, str]

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


class BasicFileMetadata:
    """The bare minimum metadata for a file.

    The metadata collected is a subset of the 'Core Metadata' documented
    in the schema: https://docs.google.com/document/d/14SanUWiYEbgarElt0YXSn_2We-rwT-ePO5Fg7rrM9lw/
    """

    def __init__(self, file: FileInfo, site: str):
        self.file = file
        self.site = site

    def generate(self) -> Dict[str, Any]:
        """Gather the file's metadata."""
        metadata = {}
        metadata["logical_name"] = self.file.path
        metadata["checksum"] = {"sha512": self.sha512sum()}  # type: ignore[assignment]
        metadata["file_size"] = self.file.stat().st_size  # type: ignore[no-untyped-call]
        metadata["locations"] = [{"site": self.site, "path": self.file.path}]  # type: ignore[assignment]
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
        filename_patterns: List[str],
    ):
        super().__init__(file, site)
        self.processing_level = processing_level
        self.meta_xml = {}  # type: Dict[str,Any]
        try:
            (
                self.season_year,
                self.run,
                self.subrun,
                self.part,
            ) = I3FileMetadata.parse_year_run_subrun_part(
                filename_patterns, self.file.name
            )
        except ValueError:
            raise Exception(
                f"Filename not in a known {self.processing_level.value} file format, {file.name}."
            )

    def generate(self) -> Dict[str, Any]:
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
    ) -> Tuple[Optional[str], Optional[str], str, Optional[List[Dict[str, Any]]]]:
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

    def _get_software(self) -> List[Dict[str, Any]]:
        """Return software metadata from `self.meta_xml`."""

        def parse_project(project: Dict[str, Any]) -> Dict[str, Any]:
            software = {}
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

    FILENAME_PATTERNS = [
        # Ex: Level2_IC86.2017_data_Run00130567_Subrun00000000_00000280.i3.zst
        # Ex: Level2pass2_IC79.2010_data_Run00115975_Subrun00000000_00000055.i3.zst
        # Ex:
        # Level2_IC86.2018RHEL_6_V05-02-00b_py2-v311_data_Run00132765_Subrun00000000_00000000.i3.zst
        r".*\.(?P<year>20\d{2}).*_data_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\.",
        #
        # Ex: Level2_PhysicsTrig_PhysicsFiltering_Run00120374_Subrun00000000_00000001.i3
        # Ex: Level2pass3_PhysicsFiltering_Run00127353_Subrun00000000_00000000.i3.gz
        # Ex: Level2_PhysicsTrig_PhysicsFiltering_Run00120374_Subrun00000000_00000001_new2.i3
        r".*_PhysicsFiltering_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)(_new\d+)?\.",
        #
        # Ex: Level2_IC86.2016_data_Run00129004_Subrun00000316.i3.bz2
        # Ex: Level2_IC86.2012_Test_data_Run00120028_Subrun00000081.i3.bz2
        # Ex: Level2_IC86.2015_24HrTestRuns_data_Run00126291_Subrun00000203.i3.bz2
        r".*\.(?P<year>20\d{2})_.*data_Run(?P<run>\d+)_Subrun(?P<part>\d+)\.",
        #
        # Ex: Level2_IC86.2011_data_Run00119221_Part00000126.i3.bz2
        r".*\.(?P<year>20\d{2})_data_Run(?P<run>\d+)_Part(?P<part>\d+)\.",
        #
        # Ex: Level2a_IC59_data_Run00115968_Part00000290.i3.gz
        # Ex: MoonEvents_Level2_IC79_data_Run00116082_NewPart00000613.i3.gz
        r".*_IC(?P<ic_strings>\d+)_data_Run(?P<run>\d+)_(New)?Part(?P<part>\d+)\.",
        #
        # Ex: Level2_All_Run00111562_Part00000046.i3.gz
        # Ex: MoonEvents_Level2_All_Run00111887_part2.i3.gz
        r".*_All_Run(?P<run>\d+)_[Pp]art(?P<part>\d+)\.",
    ]

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
        Optional[List[Dict[str, Any]]],
        Optional[float],
        Optional[Dict[str, Any]],
        Optional[Dict[str, Any]],
    ]:
        """Return formatted data points from `self.gaps_dict`."""
        if not self.gaps_dict:
            return None, None, None, None

        livetime = float(self.gaps_dict["File Livetime"])  # Ex: 0.92
        if livetime < 0:  # corrupted value, don't read any more values
            return None, None, None, None

        try:
            # Ex: '53162019 2018 206130762188498'
            first = self.gaps_dict["First Event of File"].split()

            # Ex: '53164679 2018 206139955965204'
            last = self.gaps_dict["Last Event of File"].split()

            first_id = int(first[0])
            first_dt = dataclasses.I3Time(int(first[1]), int(first[2])).date_time

            last_id = int(last[0])
            last_dt = dataclasses.I3Time(int(last[1]), int(last[2])).date_time

            gaps = [
                {
                    "start_event_id": first_id,
                    "stop_event_id": last_id,
                    "delta_time": (last_dt - first_dt).total_seconds(),
                    "start_date": first_dt.isoformat(),
                    "stop_date": last_dt.isoformat(),
                }
            ]

            first_event_dict = {"event_id": first_id, "datetime": first_dt.isoformat()}
            last_event_dict = {"event_id": last_id, "datetime": last_dt.isoformat()}

            return gaps, livetime, first_event_dict, last_event_dict

        except KeyError:
            return None, livetime, None, None

    def generate(self) -> Dict[str, Any]:
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

        Check if `filename` matches the generic filename pattern for L2
        files.
        """
        # Ex: Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188.i3.zst
        # check if last char of filename (w/o extension) is an int
        return bool(re.match(r".*Level2.*Run(\d+)_.*\d\.i3", filename))


class PFFiltFileMetadata(I3FileMetadata):
    """Metadata for PFFilt i3 files."""

    FILENAME_PATTERNS = [
        # Ex: PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        # Ex: PFFilt_PhysicsTrig_PhysicsFiltering_Run00121503_Subrun00000000_00000314.tar.bz2
        # Ex: orig.PFFilt_PhysicsFiltering_Run00127080_Subrun00000000_00000244.tar.bz2.orig
        r".*PFFilt_.*_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\.",
        #
        # Ex: PFFilt_PhysicsTrig_PhysicsFilt_Run00089959_00180.tar.gz
        # Ex: PFFilt_PhysicsTrig_RandomFilt_Run86885_006.tar.gz
        r"PFFilt_.*_Run(?P<run>\d+)_(?P<part>\d+)\.",
    ]

    def __init__(self, file: FileInfo, site: str):
        super().__init__(
            file, site, ProcessingLevel.PFFilt, PFFiltFileMetadata.FILENAME_PATTERNS
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFFilt filename.

        Check if `filename` matches the generic filename pattern for
        PFFilt files.
        """
        # Ex. PFFilt_PhysicsFiltering_Run00131989_Subrun00000000_00000295.tar.bz2
        return bool(re.match(r".*PFFilt.*Run(\d+)_.*\d\.tar\.(gz|bz2|zst)", filename))


class PFDSTFileMetadata(I3FileMetadata):
    """Metadata for PFDST i3 files."""

    FILENAME_PATTERNS = [
        # Ex. ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz
        # Ex: ukey_42c89a63-e3f7-4c3e-94ae-840eff8bd4fd_PFDST_RandomFiltering_Run00131155_Subrun00000051_00000000.tar.gz
        # Ex: PFDST_PhysicsFiltering_Run00125790_Subrun00000000_00000064.tar.gz
        # Ex: PFDST_UW_PhysicsFiltering_Run00125832_Subrun00000000_00000000.tar.gz
        # Ex: PFDST_RandomFiltering_Run00123917_Subrun00000000_00000000.tar.gz
        # Ex: PFDST_PhysicsTrig_PhysicsFiltering_Run00121663_Subrun00000000_00000091.tar.gz
        # Ex: PFDST_TestData_PhysicsFiltering_Run00122158_Subrun00000000_00000014.tar.gz
        # Ex: PFDST_TestData_RandomFiltering_Run00119375_Subrun00000136_00000000.tar.gz
        # Ex: PFDST_TestData_Unfiltered_Run00119982_Subrun00000000_000009.tar.gz
        r".*_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\."
    ]

    def __init__(self, file: FileInfo, site: str):
        super().__init__(
            file, site, ProcessingLevel.PFDST, PFDSTFileMetadata.FILENAME_PATTERNS
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFDST filename.

        Check if `filename` matches the generic filename pattern for
        PFDST files.
        """
        # Ex.
        # ukey_fa818e64-f6d2-4cc1-9b34-e50bfd036bf3_PFDST_PhysicsFiltering_Run00131437_Subrun00000000_00000066.tar.gz
        return bool(re.match(r".*PFDST.*Run(\d+)_.*\d\.tar\.(gz|bz2|zst)", filename))


class PFRawFileMetadata(I3FileMetadata):
    """Metadata for PFRaw i3 files."""

    FILENAME_PATTERNS = [
        # Ex: key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz
        # Ex: ukey_b98a353f-72e8-4d2e-afd7-c41fa5c8d326_PFRaw_PhysicsFiltering_Run00131322_Subrun00000000_00000018.tar.gz
        # Ex: ukey_05815dd9-2411-468c-9bd5-e99b8f759efd_PFRaw_RandomFiltering_Run00130470_Subrun00000060_00000000.tar.gz
        # Ex: PFRaw_PhysicsTrig_PhysicsFiltering_Run00114085_Subrun00000000_00000208.tar.gz
        # Ex: PFRaw_TestData_PhysicsFiltering_Run00114672_Subrun00000000_00000011.tar.gz
        # Ex: PFRaw_TestData_RandomFiltering_Run00113816_Subrun00000033_00000000.tar.gz
        r".*_Run(?P<run>\d+)_Subrun(?P<subrun>\d+)_(?P<part>\d+)\.",
        #
        # Ex: EvtMonPFRaw_PhysicsTrig_RandomFiltering_Run00106489_Subrun00000000.tar.gz
        r".*_Run(?P<run>\d+)_Subrun(?P<part>\d+)\.",
        #
        # Ex: DebugData_PFRaw_Run110394_1.tar.gz
        r".*_Run(?P<run>\d+)_(?P<part>\d+)\.",
        #
        # Ex: DebugData-PFRaw_RF_Run00129335_SR00_00.tar.gz.tar.gz
        r".*_Run(?P<run>\d+)_SR(?P<part>\d+)_\d+\.",
        #
        # Ex: DebugData-missing_PFRaw_data_Run129969_21_to_24.tar.gz
        r".*_Run(?P<run>\d+)_\d+_to_\d+\.",
        #
        # Ex: DebugData_PFRaw_TestData_PhysicsFiltering_Run00111448.tar.gz
        # Ex: DebugData-PFRaw_TestData_Run00118957.tar.gz
        # Ex: DebugData-PFRaw_flasher_Run130047.tar.gz
        r".*DebugData.*_Run(?P<run>\d+)\.",
        #
        # Ex: EvtMonPFRaw_PhysicsTrig_RandomFilt_Run86510.tar.gz
        r".*EvtMon.*_Run(?P<run>\d+)\.",
    ]

    def __init__(self, file: FileInfo, site: str):
        super().__init__(
            file, site, ProcessingLevel.PFRaw, PFRawFileMetadata.FILENAME_PATTERNS
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFRaw filename.

        Check if `filename` matches the generic filename pattern for
        PFRaw files.
        """
        # Ex. key_31445930_PFRaw_PhysicsFiltering_Run00128000_Subrun00000000_00000156.tar.gz
        return bool(re.match(r".*PFRaw.*Run\d+.*\d\.tar\.(gz|bz2|zst)", filename))


class MetadataManager:  # pylint: disable=R0903
    """Commander class for handling metadata for different file types."""

    def __init__(self, site: str, basic_only: bool = False):
        self.dir_path = ""
        self.site = site
        self.basic_only = basic_only
        self.l2_dir_metadata = {}  # type: Dict[str, Dict[str, Any]]

    def _prep_l2_dir_metadata(self) -> None:
        """Get metadata files for later processing with individual i3 files."""
        self.l2_dir_metadata = {}
        dir_meta_xml = None
        gaps_files = {}  # gaps_files[<filename w/o extension>]
        gcd_files = {}  # gcd_files[<run id w/o leading zeros>]

        for dir_entry in os.scandir(self.dir_path):
            if not dir_entry.is_file():
                continue

            # Meta XML (one per directory)
            # Ex. level2_meta.xml, level2pass2_meta.xml
            if re.match(r"level2.*meta\.xml$", dir_entry.name):
                if dir_meta_xml is not None:
                    raise Exception(
                        f"Multiple level2*meta.xml files found in {self.dir_path}."
                    )
                try:
                    with open(dir_entry.path, "r") as xml_file:
                        dir_meta_xml = xmltodict.parse(xml_file.read())
                        dir_meta_xml = typing.cast(Dict[str, Any], dir_meta_xml)
                    logging.debug(f"Grabbed level2*meta.xml file, {dir_entry.name}.")
                except xml.parsers.expat.ExpatError:
                    pass

            # Gaps Files (one per i3 file)
            # Ex. Run00130484_GapsTxt.tar
            elif "_GapsTxt.tar" in dir_entry.name:
                try:
                    with tarfile.open(dir_entry.path) as tar:
                        for tar_obj in tar:
                            file_dict = yaml.safe_load(tar.extractfile(tar_obj))  # type: ignore[arg-type]
                            # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt
                            no_extension = tar_obj.name.split("_gaps.txt")[0]
                            gaps_files[no_extension] = file_dict
                            logging.debug(
                                f"Grabbed gaps file for '{no_extension}', {dir_entry.name}."
                            )
                except tarfile.ReadError:
                    pass

            # GCD Files (one per run)
            # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
            elif "GCD" in dir_entry.name:
                run = I3FileMetadata.parse_run_number(dir_entry.name)
                gcd_files[str(run)] = dir_entry.path
                logging.debug(f"Grabbed GCD file for run {run}, {dir_entry.name}.")

        self.l2_dir_metadata["dir_meta_xml"] = dir_meta_xml if dir_meta_xml else {}
        self.l2_dir_metadata["gaps_files"] = gaps_files
        self.l2_dir_metadata["gcd_files"] = gcd_files

    def new_file(self, filepath: str) -> BasicFileMetadata:
        """Return different metadata-file objects.

        Factory method.
        """
        file = FileInfo(filepath)
        if not self.basic_only:
            # L2
            if L2FileMetadata.is_valid_filename(file.name):
                # get directory's metadata
                file_dir_path = os.path.dirname(os.path.abspath(file.path))
                if (not self.l2_dir_metadata) or (file_dir_path != self.dir_path):
                    self.dir_path = file_dir_path
                    self._prep_l2_dir_metadata()
                try:
                    no_extension = file.name.split(".i3")[0]
                    gaps = self.l2_dir_metadata["gaps_files"][no_extension]
                except KeyError:
                    gaps = {}
                try:
                    run = I3FileMetadata.parse_run_number(file.name)
                    gcd = self.l2_dir_metadata["gcd_files"][str(run)]
                except KeyError:
                    gcd = ""
                logging.debug(f"Gathering L2 metadata for {file.name}...")
                return L2FileMetadata(
                    file, self.site, self.l2_dir_metadata["dir_meta_xml"], gaps, gcd
                )
            # PFFilt
            if PFFiltFileMetadata.is_valid_filename(file.name):
                logging.debug(f"Gathering PFFilt metadata for {file.name}...")
                return PFFiltFileMetadata(file, self.site)
            # PFDST
            if PFDSTFileMetadata.is_valid_filename(file.name):
                logging.debug(f"Gathering PFDST metadata for {file.name}...")
                return PFDSTFileMetadata(file, self.site)
            # PFRaw
            if PFRawFileMetadata.is_valid_filename(file.name):
                logging.debug(f"Gathering PFRaw metadata for {file.name}...")
                return PFRawFileMetadata(file, self.site)
            # if no match, fall-through to BasicFileMetadata...
        # Other/ Basic
        logging.debug(f"Gathering basic metadata for {file.name}...")
        return BasicFileMetadata(file, self.site)


def sorted_unique_filepaths(
    file_of_filepaths: Optional[str] = None,
    list_of_filepaths: Optional[List[str]] = None,
) -> List[str]:
    """Return an aggregated, sorted, and set-unique list of filepaths.

    Read in lines from the `file_of_filepaths` file, and/or aggregate with those
    in `list_of_filepaths` list. Do not check if filepaths exist.

    Keyword Arguments:
        file_of_filepaths {Optional[str]} -- a file with a filepath on each line (default: {None})
        list_of_filepaths {Optional[List[str]]} -- a list of filepaths (default: {None})

    Returns:
        List[str] -- all unique filepaths
    """

    def convert_to_good_string(b_string: bytes) -> Optional[str]:
        # strip trailing new-line char
        if b_string[-1] == ord("\n"):
            b_string = b_string[:-1]
        # ASCII parse
        for b_char in b_string:
            if not (ord(" ") <= b_char <= ord("~")):  # pylint: disable=C0325
                logging.info(
                    f"Invalid filename, {b_string!r}, has special character(s)."
                )
                return None
        # Decode UTF-8
        try:
            path = b_string.decode("utf-8", "strict").rstrip()
        except UnicodeDecodeError as e:
            logging.info(f"Invalid filename, {b_string!r}, {e.__class__.__name__}.")
            return None
        # Non-printable chars
        if not set(path).issubset(string.printable):
            logging.info(f"Invalid filename, {path}, has non-printable character(s).")
            return None
        # all good
        return path

    filepaths = []
    if list_of_filepaths:
        filepaths.extend(list_of_filepaths)
    if file_of_filepaths:
        with open(file_of_filepaths, "rb") as bin_file:
            for bin_line in bin_file:
                path = convert_to_good_string(bin_line)
                if path:
                    filepaths.append(path)

    filepaths = [f for f in sorted(set(filepaths)) if f]
    return filepaths


async def request_post_patch(
    fc_rc: RestClient, metadata: Dict[str, Any], dont_patch: bool = False
) -> RestClient:
    """POST metadata, and PATCH if file is already in the file catalog."""
    try:
        _ = await fc_rc.request("POST", "/api/files", metadata)
        logging.debug("POSTed.")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 409:
            if dont_patch:
                logging.debug("File already exists, not replacing.")
            else:
                patch_path = e.response.json()["file"]  # /api/files/{uuid}
                _ = await fc_rc.request("PATCH", patch_path, metadata)
                logging.debug("PATCHed.")
        else:
            raise
    return fc_rc


async def process_file(
    filepath: str, manager: MetadataManager, fc_rc: RestClient, no_patch: bool
) -> None:
    """Gather and POST metadata for a file."""
    try:
        metadata_file = manager.new_file(filepath)
        metadata = metadata_file.generate()
    # OSError is thrown for special files like sockets
    except (OSError, PermissionError, FileNotFoundError) as e:
        logging.exception(f"{filepath} not gathered, {e.__class__.__name__}.")
        return
    except:  # noqa: E722
        logging.exception(f"Unexpected exception raised for {filepath}.")
        raise

    logging.debug(f"{filepath} gathered.")
    logging.debug(metadata)
    await request_post_patch(fc_rc, metadata, no_patch)


def fix_known_filepath_issues(filepath: str) -> Optional[List[str]]:
    """Deal with known weird quirks in filenames."""
    # split filenames that were concatenated at some point in preprocessing
    match = re.match(r"(?P<first>/data/exp/.*)(?P<second>/data/exp/.*)", filepath)
    if match:
        files = list(match.groupdict().values())
        files = sorted_unique_filepaths(list_of_filepaths=files)
        return files
    return None


async def process_paths(
    paths: List[str], manager: MetadataManager, fc_rc: RestClient, no_patch: bool
) -> List[str]:
    """POST metadata of files given by paths, and return any directories."""
    sub_files = []  # type: List[str]

    for p in paths:
        try:
            if is_processable_path(p):
                if os.path.isfile(p):
                    await process_file(p, manager, fc_rc, no_patch)
                elif os.path.isdir(p):
                    logging.debug(f"Directory found, {p}. Queuing its contents...")
                    sub_files.extend(
                        dir_entry.path
                        for dir_entry in os.scandir(p)
                        if not dir_entry.is_symlink()
                    )  # don't add symbolic links
            else:
                logging.info(f"Skipping {p}, not a directory nor file.")

        except (PermissionError, FileNotFoundError) as e:
            logging.info(f"Skipping {p}, {e.__class__.__name__}.")

        except NotADirectoryError as e:
            fixed_filepaths = fix_known_filepath_issues(p)
            if fixed_filepaths:
                paths.extend(fixed_filepaths)
                logging.info(
                    f"Fixed known issue with filepath, {p} -> {fixed_filepaths}."
                )
            else:
                logging.info(f"Skipping {p}, {e.__class__.__name__}.")

    return sub_files


def path_in_blacklist(path: str, blacklist: List[str]) -> bool:
    """Return `True` if `path` is blacklisted.

    Either:
    - `path` is in `blacklist`, or
    - `path` has a parent path in `blacklist`.
    """
    for bad_path in blacklist:
        if (path == bad_path) or (os.path.commonpath([path, bad_path]) == bad_path):
            logging.debug(
                f"Skipping {path}, file and/or directory path is blacklisted ({bad_path})."
            )
            return True
    return False


def process_work(
    paths: List[str],
    blacklist: List[str],
    rest_client_args: RestClientArgs,
    site: str,
    indexer_flags: IndexerFlags,
) -> List[str]:
    """Wrap async function, `process_paths`.

    Return files nested under any directories.
    """
    if not isinstance(paths, list):
        raise TypeError(f"`paths` object is not list {paths}")
    if not paths:
        return []

    # Check blacklist
    paths = [p for p in paths if not path_in_blacklist(p, blacklist)]

    # Process Paths
    fc_rc = RestClient(
        rest_client_args["url"],
        token=rest_client_args["token"],
        timeout=rest_client_args["timeout"],
        retries=rest_client_args["retries"],
    )
    manager = MetadataManager(site, indexer_flags["basic_only"])
    sub_files = asyncio.get_event_loop().run_until_complete(
        process_paths(paths, manager, fc_rc, indexer_flags["no_patch"])
    )

    fc_rc.close()
    return sub_files


def check_path(path: str) -> None:
    """Check if `path` is rooted at a white-listed root path."""
    for root in ACCEPTED_ROOTS:
        if root == os.path.commonpath([path, root]):
            return
    message = f"{path} is not rooted at: {', '.join(ACCEPTED_ROOTS)}"
    logging.critical(message)
    raise Exception(f"Invalid path ({message}).")


def gather_file_info(  # pylint: disable=R0913
    starting_paths: List[str],
    blacklist: List[str],
    rest_client_args: RestClientArgs,
    site: str,
    indexer_flags: IndexerFlags,
    processes: int,
) -> None:
    """Gather and post metadata from files rooted at `starting_paths`.

    Do this multi-processed.
    """
    # Get full paths
    starting_paths = [os.path.abspath(p) for p in starting_paths]
    for p in starting_paths:
        check_path(p)

    # Traverse paths and process files
    futures = []  # type: List[Future]  # type: ignore[type-arg]
    with ProcessPoolExecutor() as pool:
        queue = starting_paths
        split = math.ceil(len(queue) / processes)
        while futures or queue:
            logging.debug(f"Queue: {len(queue)}.")
            # Divvy up queue among available worker(s). Each worker gets 1/nth of the queue.
            if queue:
                queue = sorted_unique_filepaths(list_of_filepaths=queue)
                while processes != len(futures):
                    paths, queue = queue[:split], queue[split:]
                    logging.debug(
                        f"Worker Assigned: {len(futures)+1}/{processes} ({len(paths)} paths)."
                    )
                    futures.append(
                        pool.submit(
                            process_work,
                            paths,
                            blacklist,
                            rest_client_args,
                            site,
                            indexer_flags,
                        )
                    )
            logging.debug(f"Workers: {len(futures)} {futures}.")
            # Extend the queue
            # concurrent.futures.wait(FIRST_COMPLETED) is slower
            while not futures[0].done():
                sleep(0.1)
            future = futures.pop(0)
            result = future.result()
            if result:
                queue.extend(result)
                split = math.ceil(len(queue) / processes)
            logging.debug(f"Worker finished: {future} (enqueued {len(result)}).")


def main() -> None:
    """Traverse paths, recursively, and index."""
    parser = argparse.ArgumentParser(
        description="Find files under PATH(s), compute their metadata and "
        "upload it to File Catalog.",
        epilog="Notes: (1) symbolic links are never followed.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "paths", metavar="PATHS", nargs="*", help="path(s) to scan for files."
    )
    parser.add_argument(
        "--paths-file",
        dest="paths_file",
        default=None,
        help="file containing path(s) to scan for files. (use this option for a large number of paths)",
    )
    parser.add_argument(
        "--processes",
        type=int,
        default=1,
        help="number of processes for multi-processing",
    )
    parser.add_argument(
        "-u",
        "--url",
        default="https://file-catalog.icecube.wisc.edu/",  # 'http://localhost:8888'
        help="File Catalog URL",
    )
    parser.add_argument(
        "-s", "--site", required=True, help='site value of the "locations" object'
    )
    parser.add_argument("-t", "--token", required=True, help="LDAP token")
    parser.add_argument(
        "--timeout", type=int, default=15, help="REST client timeout duration"
    )
    parser.add_argument(
        "--retries", type=int, default=3, help="REST client number of retries"
    )
    parser.add_argument(
        "--basic-only",
        dest="basic_only",
        default=False,
        action="store_true",
        help="only collect basic metadata",
    )
    parser.add_argument(
        "--no-patch",
        dest="no_patch",
        default=False,
        action="store_true",
        help="do not PATCH if the file already exists in the file catalog",
    )
    parser.add_argument(
        "--blacklist-file",
        dest="blacklist_file",
        help="blacklist file containing all paths to skip",
    )
    parser.add_argument("-l", "--log", default="DEBUG", help="the output logging level")
    args = parser.parse_args()

    logging.basicConfig(level=getattr(logging, args.log.upper()))
    for arg, val in vars(args).items():
        logging.info(f"{arg}: {val}")

    logging.info(
        f"Collecting metadata from {args.paths} and those in file (at {args.paths_file})..."
    )

    # Aggregate and sort all paths
    paths = sorted_unique_filepaths(
        file_of_filepaths=args.paths_file, list_of_filepaths=args.paths
    )

    # Read blacklisted paths
    blacklist = sorted_unique_filepaths(file_of_filepaths=args.blacklist_file)

    # Grab and pack args
    rest_client_args = {
        "url": args.url,
        "token": args.token,
        "timeout": args.timeout,
        "retries": args.retries,
    }  # type: RestClientArgs
    indexer_flags = {
        "basic_only": args.basic_only,
        "no_patch": args.no_patch,
    }  # type: IndexerFlags

    # Go!
    gather_file_info(
        paths, blacklist, rest_client_args, args.site, indexer_flags, args.processes
    )


if __name__ == "__main__":
    main()
