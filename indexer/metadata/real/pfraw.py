"""Class for collecting PFRaw file metadata."""


import re
from typing import List

from ...utils import utils
from . import filename_patterns
from .data_exp import DataExpI3FileMetadata

try:
    from typing import Final
except ImportError:
    from typing_extensions import Final  # type: ignore[misc]


class PFRawFileMetadata(DataExpI3FileMetadata):
    """Metadata for PFRaw i3 files."""

    FILENAME_PATTERNS: Final[List[str]] = filename_patterns.PFRaw["patterns"]

    def __init__(self, file: utils.FileInfo, site: str):
        super().__init__(
            file, site, utils.ProcessingLevel.PFRaw, PFRawFileMetadata.FILENAME_PATTERNS
        )
        self._grab_meta_xml_from_tar()

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid PFRaw filename.

        Check if `filename` matches the base filename pattern for PFRaw
        files.
        """
        return bool(re.match(filename_patterns.PFRaw["base_pattern"], filename))
