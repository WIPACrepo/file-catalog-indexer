"""Class for collecting simulation (/data/sim/) i3 file metadata."""

import re
from typing import List, Optional, Tuple

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
        try:
            (
                self.iceprod_dataset_id,
                self.iceprod_job_id,
            ) = DataSimI3FileMetadata.parse_iceprod_dataset_job_ids(
                regexes, self.file.name
            )
        except ValueError:
            raise Exception(f"Unaccounted for /data/sim/ filename pattern: {file.name}")

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

    @staticmethod
    def parse_iceprod_dataset_job_ids(
        regexes: List[re.Pattern[str]], filename: str
    ) -> Tuple[Optional[int], Optional[int]]:
        """Return the iceprod dataset and job ids by parsing w/ `regexes`.

        Uses named groups: `alpha` & `beta`; or `single`.
        """
        for p in regexes:
            match = re.match(p, filename)
            if not match:
                continue

            values = match.groupdict()
            # pattern w/ no groups
            if not values:
                return None, None
            # pattern w/ 'single' group
            if "single" in values:
                return int(values["single"]), None
            # pattern w/ 'alpha' & 'beta' groups
            return int(values["alpha"]), int(values["beta"])

        # fall-through
        raise ValueError(f"Filename does not match any pattern, {filename}.")

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata = super().generate()

        # TODO -- grab what I can from i3Reader, should that go here or up in i3.py?

        iceprod_task_id: Optional[int] = None

        if not self.iceprod_dataset_id:
            pass  # TODO
        # IceProd v1 (the old one)
        elif self.iceprod_dataset_id < 20000:
            metadata["simulation"] = self.iceprod_1()  # TODO
        # IceProd v2
        else:
            metadata["simulation"] = self.iceprod_2()  # TODO

        if self.iceprod_dataset_id:
            metadata["iceprod"] = {
                "dataset": "",
                "dataset_id": self.iceprod_dataset_id,
                "job": "",
                "job_id": self.iceprod_job_id,
                "task": "",
                "task_id": iceprod_task_id,
                "config": "",
            }

        return metadata

    @staticmethod
    def is_valid_filename(filename: str) -> bool:
        """Return `True` if the file is a valid simulation i3 filename.

        Check if `filename` matches the base filename pattern for
        simulation i3 files.
        """
        if not any(
            filename.endswith(x) for x in [".i3", ".i3.gz", ".i3.bz2", ".i3.zst"]
        ):
            return False

        # hard-coded ignore
        # Ex: Level2_IC86.2015_data_Run00126515_Subrun00000191.i3.bz2
        if "Run" in filename and "Subrun" in filename:
            return False

        return True
