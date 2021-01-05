"""Class for collecting i3 file metadata."""


from ..utils import utils
from .basic import BasicFileMetadata


class I3FileMetadata(BasicFileMetadata):
    """Metadata for i3 files."""

    def __init__(self, file: utils.FileInfo, site: str):
        super().__init__(file, site)
