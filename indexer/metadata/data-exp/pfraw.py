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
