"""Class for collecting PFFilt file metadata."""


import re
from typing import List

from ...utils import utils
from . import filename_patterns
from .data_exp import DataExpI3FileMetadata

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final  # type: ignore[misc]


class PFFiltFileMetadata(DataExpI3FileMetadata):
    """Metadata for PFFilt i3 files."""

    FILENAME_PATTERNS: Final[List[str]] = filename_patterns.PFFilt["patterns"]

    def __init__(self, file: utils.FileInfo, site: str):
        super().__init__(
            file,
            site,
            utils.ProcessingLevel.PFFilt,
            PFFiltFileMetadata.FILENAME_PATTERNS,
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFFilt filename.

        Check if `filename` matches the base filename pattern for PFFilt
        files.
        """
        return bool(re.match(filename_patterns.PFFilt["base_pattern"], filename))
