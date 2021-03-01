"""Class for collecting L2 file metadata."""


import datetime
import re
from typing import Any, cast, Dict, List, Optional, Tuple

from file_catalog.schema import types

from ...utils import utils
from . import filename_patterns
from .data_exp import DataExpI3FileMetadata

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final  # type: ignore[misc]


class L2FileMetadata(DataExpI3FileMetadata):
    """Metadata for L2 i3 files."""

    FILENAME_PATTERNS: Final[List[str]] = filename_patterns.L2["patterns"]

    def __init__(  # pylint: disable=R0913
        self,
        file: utils.FileInfo,
        site: str,
        dir_meta_xml: Dict[str, Any],
        gaps_dict: Dict[str, Any],
        gcd_filepath: str,
    ):
        super().__init__(
            file, site, utils.ProcessingLevel.L2, L2FileMetadata.FILENAME_PATTERNS
        )
        self.meta_xml = dir_meta_xml
        self.gaps_dict = gaps_dict
        self.gcd_filepath = gcd_filepath

    @staticmethod
    def _i3time_to_datetime(year: int, daq_time: int) -> datetime.datetime:
        """Convert `I3Time` to `datetime.datetime`."""
        from icecube import dataclasses  # type: ignore[import]  # pylint: disable=E0401,C0415

        i3_dt = dataclasses.I3Time(year, daq_time).date_time
        return cast(datetime.datetime, i3_dt)

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

        try:
            # Ex: '53162019 2018 206130762188498'
            first = self.gaps_dict["First Event of File"].split()

            # Ex: '53164679 2018 206139955965204'
            last = self.gaps_dict["Last Event of File"].split()

            first_id = int(first[0])
            first_dt = self._i3time_to_datetime(int(first[1]), int(first[2]))

            last_id = int(last[0])
            last_dt = self._i3time_to_datetime(int(last[1]), int(last[2]))

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
            "season_name": utils.IceCubeSeason.year_to_name(self.season_year),
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
