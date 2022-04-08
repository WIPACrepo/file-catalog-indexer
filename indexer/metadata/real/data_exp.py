"""Class for collecting real (/data/exp/) i3 file metadata."""


import collections
import logging
import os
import re
import tarfile
import xml
import zlib
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

import xmltodict  # type: ignore[import]
from file_catalog.schema import types

from ...utils import utils
from ..i3 import I3FileMetadata


class DataExpI3FileMetadata(I3FileMetadata):
    """Metadata for /data/exp/ i3 files."""

    def __init__(
        self,
        file: utils.FileInfo,
        site: str,
        processing_level: utils.ProcessingLevel,
        filename_patterns_: List[str],
    ):
        super().__init__(file, site, processing_level, "real")
        if not self.processing_level:
            raise TypeError(
                "Processing level cannot be None for DataExpI3FileMetadata and derived instances."
            )
        self.meta_xml: Dict[str, Any] = {}
        try:
            (
                self.season_year,
                self.run,
                self.subrun,
                self.part,
            ) = DataExpI3FileMetadata.parse_year_run_subrun_part(
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

        metadata["create_date"] = create_date  # Override BasicFileMetadata's value
        metadata["software"] = software

        metadata["run"] = {
            "run_number": self.run,
            "subrun_number": self.subrun,
            "part_number": self.part,
            "start_datetime": start_dt,
            "end_datetime": end_dt,
            "first_event": self._get_events_data()["first_event"],
            "last_event": self._get_events_data()["last_event"],
            "event_count": self._get_events_data()["event_count"],
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
                    year = utils.IceCubeSeason.name_to_year(f"IC{values['ic_strings']}")
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
