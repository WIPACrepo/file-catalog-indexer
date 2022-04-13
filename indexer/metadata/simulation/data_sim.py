"""Class for collecting simulation (/data/sim/) i3 file metadata."""


import logging
import re
from typing import Dict, List, Optional, Pattern, Tuple

from file_catalog.schema import types

from ...utils import utils
from ..i3 import I3FileMetadata
from .iceprod_tools import (
    DatasetNotFound,
    IceProdConnection,
    SteeringParameters,
    get_steering_params_and_ip_metadata,
)


class DataSimI3FileMetadata(I3FileMetadata):
    """Metadata for /data/sim/ i3 files."""

    def __init__(  # pylint: disable=R0913
        self,
        file: utils.FileInfo,
        site: str,
        regexes: List[Pattern[str]],
        iceprod_conn: IceProdConnection,
    ):
        super().__init__(
            file,
            site,
            DataSimI3FileMetadata.figure_processing_level(file),
            "simulation",
        )
        self.iceprod_conn = iceprod_conn
        try:
            (
                self.iceprod_dataset_num,
                self.iceprod_job_index,
            ) = DataSimI3FileMetadata.parse_iceprod_dataset_job_ids(regexes, self.file)
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
            utils.ProcessingLevel.L5: ["L5"],  # implicitly, also "level5"
            utils.ProcessingLevel.L4: ["L4"],  # ``
            utils.ProcessingLevel.L3: ["L3"],  # ``
            utils.ProcessingLevel.L2: ["L2"],  # ``
            utils.ProcessingLevel.L1: ["L1"],  # ``
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
        regexes: List[Pattern[str]], file: utils.FileInfo
    ) -> Tuple[Optional[int], Optional[int]]:
        """Return the iceprod dataset_num and job_index, via `regexes`.

        Uses named groups: `alpha` & `beta`; or `single`.
        """
        for pattern in regexes:
            match = re.match(pattern, file.name)
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
        raise ValueError(f"Filename does not match any pattern, {file.path}.")

    @staticmethod
    def get_simulation_metadata(  # pylint: disable=R0912
        steering_parameters: SteeringParameters, iceprod_dataset_num: int
    ) -> types.SimulationMetadata:
        """Gather "simulation" metadata from steering parameters."""

        def add_keys_to_list(dict_: Dict[str, List[str]]) -> Dict[str, List[str]]:
            """Add each key, itself, to its own list."""
            for key in dict_:
                dict_[key].append(key)
            return dict_

        metakey_substrings: Dict[str, List[str]] = add_keys_to_list(
            {
                "generator": ["category"],  # str # 1st Choice
                "generator-backup": ["mctype"],  # 2nd Choice # "MCType" "mctype"
                "composition": [
                    "flavor"
                ],  # str # "GENIE::flavor" "NUGEN::flavor" "GENERATION::nugen_flavor"
                "geometry": [],  # str
                "GCD_file": ["gcd"],  # str # "gcdfile", "gcdpass2" "gcdfile_11"
                "bulk_ice_model": ["IceModel", "bulkice"],  # str # "icemodel"
                "hole_ice_model": ["holeice"],  # str
                "photon_propagator": ["photonpropagator"],  # str
                "DOMefficiency": [],  # float # "DOMefficiency::0" "DOMefficiency::1"
                "atmosphere": ["atmod", "ratmo"],  # int # "CORSIKA::atmod"
                "n_events": [
                    "nevents",
                    "NumberOfPrimaries",
                    "showers",
                ],  # int "GENERATION::n_events" "CORSIKA::showers"
                "oversampling": [],  # int # "CORSIKA::oversampling"
                "DOMoversize": ["oversize"],  # int # "oversize", "CLSIM::OVERSIZE"
                "energy_min": [
                    "eprimarymin",
                    "emin",
                    "e_min",
                ],  # str # "CORSIKA::eprimarymin" "GENIE::emin" "NUGEN::emin" "GENERATION::e_min"
                "energy_max": [
                    "eprimarymax",
                    "emax",
                    "e_max",
                ],  # float # "CORSIKA::eprimarymax" "GENIE::emax" "NUGEN::emax" "GENERATION::e_max"
                "power_law_index": [
                    "spectrum",
                    "gamma",
                    "eslope",
                ],  # float # "CORSIKA::spectrum" "NUGEN::gamma" "CORSIKA::eslope"
                "cylinder_length": [
                    "length"
                ],  # float # "MMC::length" "CORSIKA::length"
                "cylinder_radius": [
                    "radius"
                ],  # float # "MMC::radius" "CORSIKA::radius"
                "zenith_min": [
                    "zenithmin"
                ],  # float # "NUGEN::zenithmin" "GENIE::zenithmin"
                "zenith_max": [
                    "zenithmax"
                ],  # float # "NUGEN::zenithmax" "GENIE::zenithmax"
                "hadronic_interaction": [
                    "hadronicinteraction"
                ],  # str # "hadronicinteraction"
            }
        )

        # Make mapping of metadata key -> steering parameter key
        key_maps: Dict[str, str] = {}
        for paramkey in sorted(steering_parameters.keys()):
            for metakey in metakey_substrings.keys():  # pylint: disable=C0201
                # case-insensitive substring search
                if not any(
                    s.upper() in paramkey.upper() for s in metakey_substrings[metakey]
                ):
                    continue
                # if there is already a mapping, only replace w/ a shorter one
                # Ex: "IceModelTarball::0" vs "IceModel"
                if (metakey in key_maps) and (len(paramkey) >= len(key_maps[metakey])):
                    continue
                key_maps[metakey] = paramkey

        # Remedy backup generator
        # HACK -- trying to keep the mapping simple here...
        # but needed a way to sneak in an order of precedence
        if "generator" not in key_maps:
            if "generator-backup" in key_maps:  # use 2nd Choice
                key_maps["generator"] = key_maps["generator-backup"]
        key_maps.pop("generator-backup", None)

        # Populate metadata
        sim_meta: types.SimulationMetadata = {}
        for metakey, paramkey in key_maps.items():
            sim_meta[metakey] = steering_parameters[paramkey]  # type: ignore[literal-required]

        # Format
        if "power_law_index" in sim_meta:
            try:  # if power_law_index is just a number, format it: E^-N
                if float(sim_meta["power_law_index"]) > 0:
                    # positive -> came from "gamma" value
                    sim_meta["power_law_index"] = f"E^-{sim_meta['power_law_index']}"
                else:
                    # negative -> came from number-formatted "spectrum"/"eslope" value
                    sim_meta["power_law_index"] = f"E^{sim_meta['power_law_index']}"
            except ValueError:
                # assuming already in "E^-N" format
                pass

        # try to cast each value - but store even if it is wrong
        for key, val in list(sim_meta.items()):
            type_ = types.simulation_metadata_types[key]
            try:
                sim_meta[key] = type_(val)  # type: ignore[literal-required]
            except (ValueError, TypeError):
                logging.debug(
                    f'Wrong data type stored for "simulation" key, ({key}:{val}) '
                    f"should be {type_} (dataset:{iceprod_dataset_num})"
                )

        return sim_meta

    def generate(self) -> types.Metadata:
        """Gather the file's metadata."""
        metadata = super().generate()

        # get IceProd dataset steering parameters and IceProdMetadata
        try:
            steering_parameters, ip_metadata = get_steering_params_and_ip_metadata(
                self.iceprod_dataset_num,
                self.file.path,
                self.iceprod_job_index,
                self.iceprod_conn,
            )

        except DatasetNotFound:
            logging.warning(
                f"Dataset {self.iceprod_dataset_num} not found. "
                f"No IceProd/Simulation metadata recorded for {self.file.path}."
            )
            return metadata

        # override self.iceprod_dataset_num w/ iceprod_tool's?
        if ip_metadata["dataset"] != self.iceprod_dataset_num:
            logging.info(
                f"Original IceProd dataset #{self.iceprod_dataset_num} was bad. "
                f"Now using #{ip_metadata['dataset']}, from parsing the filepath ({self.file.path})"
            )
            self.iceprod_dataset_num = ip_metadata["dataset"]

        # parse steering parameters -> SimulationMetadata
        sim_metadata = self.get_simulation_metadata(
            steering_parameters, self.iceprod_dataset_num
        )

        # update
        metadata.update({"iceprod": ip_metadata, "simulation": sim_metadata})

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
        # Ex: IC86_Merged_Muons_Emin_0.500000_TeV_Emax_10.000000_PeV_Gamma_6.000000_RunNumber_1152_Seed_144244.i3.bz2
        if "RunNumber" in filename:
            return False

        return True
