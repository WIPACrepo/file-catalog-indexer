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
