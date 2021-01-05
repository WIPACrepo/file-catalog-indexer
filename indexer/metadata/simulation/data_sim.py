"""Class for collecting simulation (/data/sim/) i3 file metadata."""

import re
from typing import List

from ...utils import utils
from ..i3 import I3FileMetadata


class DataSimI3FileMetadata(I3FileMetadata):
    """Metadata for /data/sim/ i3 files."""

    def __init__(self, file: utils.FileInfo, site: str):
        super().__init__(file, site)

    @staticmethod
    def is_valid_filename(filename: str, regexes: List[re.Pattern[str]]) -> bool:
        """Return `True` if the file is a valid simulation i3 filename.

        Check if `filename` matches the base filename pattern for
        simulation i3 files.
        """
        if not any(
            filename.endswith(x) for x in [".i3", ".i3.gz", ".i3.bz2", ".i3.zst"]
        ):  # TODO -- test this
            return False

        # hard-coded ignore
        # Ex: Level2_IC86.2015_data_Run00126515_Subrun00000191.i3.bz2
        if "Run" in filename and "Subrun" in filename:
            return False

        return True

        raise Exception(f"Unaccounted for /data/sim/ filename pattern: {filename}")
