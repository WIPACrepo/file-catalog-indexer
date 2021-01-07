"""Class for collecting simulation (/data/sim/) i3 file metadata."""

import re
from typing import List, Optional

from ...utils import types, utils
from ..i3 import I3FileMetadata


class DataSimI3FileMetadata(I3FileMetadata):
    """Metadata for /data/sim/ i3 files."""

    def __init__(self, file: utils.FileInfo, site: str, regexes: List[re.Pattern[str]]):
        super().__init__(
            file,
            site,
            DataSimI3FileMetadata.figure_processing_level(file),
            "simulation",
        )
        self.regexes = regexes
        raise Exception(f"Unaccounted for /data/sim/ filename pattern: {filename}")

    @staticmethod
    def figure_processing_level(
        file: utils.FileInfo,
    ) -> Optional[utils.ProcessingLevel]:
        """Get the processing level from the filename."""
        fname_upper = file.name.upper()

        # L5 - L1 -> Triggered -> Propagated -> Generated
        proc_level_strings = {
            utils.ProcessingLevel.L5: ["L5"],
            utils.ProcessingLevel.L4: ["L4"],
            utils.ProcessingLevel.L3: ["L3"],
            utils.ProcessingLevel.L2: ["L2"],
            utils.ProcessingLevel.L1: ["L1"],
            utils.ProcessingLevel.Triggered: ["detector"],
            utils.ProcessingLevel.Propagated: ["hits", "hit", "propagated"],
            utils.ProcessingLevel.Generated: [
                "corsika",
                "unweighted",
                "nugen",
                "injector",  # MCSNInjector, SimpleInjector, lepton-injector
                "genie",
                "generated",
                "numu",
                "nue",
                "nutau",
                "muongun",
                "Monopole",
                "MonoSim",
            ],
        }
        for proc_level, strings in proc_level_strings.items():
            if any(t.upper() in fname_upper for t in strings):
                return proc_level

        return None

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata = super().generate()

        # TODO

        return metadata

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
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
