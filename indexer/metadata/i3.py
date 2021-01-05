"""Class for collecting i3 file metadata."""


import collections
import logging
import os
import re
import tarfile
import typing
from datetime import date
from typing import Any, Dict, List, Optional, Tuple

from ..utils import types, utils
from .basic import BasicFileMetadata


class I3FileMetadata(BasicFileMetadata):
    """Metadata for i3 files."""

    def __init__(
        self,
        file: utils.FileInfo,
        site: str,
        processing_level: utils.ProcessingLevel,
        data_type: str,
    ):
        super().__init__(file, site)
        self.processing_level = processing_level
        self.data_type = data_type

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

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata = super().generate()

        metadata["data_type"] = self.data_type
        metadata["processing_level"] = self.processing_level.value

        return metadata
