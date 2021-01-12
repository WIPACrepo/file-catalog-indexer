"""Class for collecting simulation (/data/sim/) i3 file metadata."""
import asyncio
import copy
import re
from typing import List, Optional, Tuple

# local imports
from iceprod.core import dataclasses as dc  # type: ignore[import]
from rest_tools.client import RestClient  # type: ignore[import]

from ...utils import types, utils
from ..i3 import I3FileMetadata
from . import iceprod_tools


class DataSimI3FileMetadata(I3FileMetadata):
    """Metadata for /data/sim/ i3 files."""

    def __init__(
        self,
        file: utils.FileInfo,
        site: str,
        regexes: List[re.Pattern[str]],
        iceprod_rc: RestClient,
    ):
        super().__init__(
            file,
            site,
            DataSimI3FileMetadata.figure_processing_level(file),
            "simulation",
        )
        self.iceprod_task_id: Optional[int] = None
        self.iceprod_rc = iceprod_rc
        try:
            (
                self.iceprod_dataset_num,
                self.iceprod_job_index,
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

    def get_simulation_metadata(dataset_config: dc.Job) -> types.SimulationMetadata:
        """Gather "simulation" metadata from steering parameters."""
        sim_meta: types.SimulationMetadata = {}
        steering_parameters_raw = iceprod_tools.get_steering_paramters(dataset_config)
        steering_parameters_upkeys = {
            k.upper(): copy.deepcopy(v) for k, v in steering_parameters_raw.items()
        }

        metakey_spkeylist = {
            "generator": ["generator"],  # str
            "composition": ["composition"],  # str
            "geometry": ["geometry"],  # str
            "GCD_file": ["GCD_file", "gcdfile", "gcdpass2"],  # str
            "bulk_ice_model": ["bulk_ice_model"],  # str
            "hole_ice_model": ["hole_ice_model"],  # str
            "photon_propagator": ["photon_propagator"],  # str
            "DOMefficiency": ["DOMefficiency"],  # float
            "atmosphere": ["atmosphere"],  # int
            "n_events": ["n_events"],  # int
            "oversampling": ["oversampling"],  # int
            "energy_min": ["energy_min"],  # float
            "energy_max": ["energy_max"],  # float
            "power_law_index": ["power_law_index"],  # float
            "cylinder_length": ["cylinder_length"],  # float
            "cylinder_radius": ["cylinder_radius"],  # float
            "zenith_min": ["zenith_min"],  # float
            "zenith_max": ["zenith_max"],  # float
        }

        for metakey, spkeylist in metakey_spkeylist.items():
            for spkey in spkeylist:
                try:
                    sim_meta[metakey] = steering_parameters_upkeys[spkey.upper()]  # type: ignore[misc]
                    break
                except KeyError:
                    continue

        return sim_meta

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata = super().generate()

        # TODO -- grab what I can from i3Reader, should that go here or up in i3.py?

        try:
            dataset_config: dc.Job = asyncio.run(
                iceprod_tools.get_dataset_config(
                    self.iceprod_rc, self.file.path, self.iceprod_dataset_num
                )
            )
        except iceprod_tools.DatasetNotFound:
            return metadata  # TODO

        # IceProd
        metadata["iceprod"] = asyncio.run(
            iceprod_tools.get_file_info(
                self.iceprod_rc,
                self.file.path,
                dataset_config,
                job_index=self.iceprod_job_index,
            )
        )

        # Simulation
        metadata["simulation"] = DataSimI3FileMetadata.get_simulation_metadata(
            dataset_config
        )

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
