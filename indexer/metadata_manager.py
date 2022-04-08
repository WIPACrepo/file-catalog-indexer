"""Class for managing metadata collection / interfacing with indexer.py."""

import logging
import os
import re
import tarfile
import typing
import xml
from typing import Any, Dict, List, Optional, Pattern

import xmltodict  # type: ignore[import]
import yaml

from .metadata import basic, real, simulation
from .metadata.simulation.data_sim import DataSimI3FileMetadata
from .metadata.simulation.iceprod_tools import IceProdConnection
from .utils import utils


class MetadataManager:  # pylint: disable=R0903
    """Commander class for handling metadata for different file types."""

    def __init__(  # pylint: disable=R0913
        self,
        site: str,
        basic_only: bool = False,
        iceprodv2_rc_token: str = "",
        iceprodv1_db_pass: str = "",
    ):
        self.dir_path = ""
        self.site = site
        self.basic_only = basic_only
        self.real_l2_dir_metadata: Dict[str, Dict[str, Any]] = {}
        self.sim_regexes: List[Pattern[str]] = []
        if not iceprodv1_db_pass and not iceprodv2_rc_token:
            self.iceprod_conn: Optional[IceProdConnection] = None
        else:
            self.iceprod_conn = IceProdConnection(iceprodv1_db_pass, iceprodv2_rc_token)

    def _new_file_basic_only(self, filepath: str) -> basic.BasicFileMetadata:
        """Return basic metadata-file object for files.

        Factory method.
        """
        file = utils.FileInfo(filepath)
        logging.debug(f"Gathering basic metadata for {file.name}...")
        return basic.BasicFileMetadata(file, self.site)

    def _real_prep_l2_dir_metadata(self) -> None:
        """Get metadata files for later processing with individual i3 files."""
        self.real_l2_dir_metadata = {}
        dir_meta_xml = None
        gaps_files = {}  # gaps_files[<filename w/o extension>]
        gcd_files = {}  # gcd_files[<run id w/o leading zeros>]

        for dir_entry in os.scandir(self.dir_path):
            if not dir_entry.is_file():
                continue

            # Meta XML (one per directory)
            # Ex. level2_meta.xml, level2pass2_meta.xml
            if re.match(r"level2.*meta\.xml$", dir_entry.name):
                if dir_meta_xml is not None:
                    raise Exception(
                        f"Multiple level2*meta.xml files found in {self.dir_path}."
                    )
                try:
                    with open(dir_entry.path, "r") as xml_file:
                        dir_meta_xml = xmltodict.parse(xml_file.read())
                        dir_meta_xml = typing.cast(Dict[str, Any], dir_meta_xml)
                    logging.debug(f"Grabbed level2*meta.xml file, {dir_entry.name}.")
                except xml.parsers.expat.ExpatError:
                    pass

            # Gaps Files (one per i3 file)
            # Ex. Run00130484_GapsTxt.tar
            elif "_GapsTxt.tar" in dir_entry.name:
                try:
                    with tarfile.open(dir_entry.path) as tar:
                        for tar_obj in tar:
                            # pylint: disable=C0325
                            iobytes = tar.extractfile(tar_obj)
                            if not iobytes:
                                continue
                            file_dict = yaml.safe_load(iobytes)
                            # Ex. Level2_IC86.2017_data_Run00130484_Subrun00000000_00000188_gaps.txt
                            no_extension = tar_obj.name.split("_gaps.txt")[0]
                            gaps_files[no_extension] = file_dict
                            logging.debug(
                                f"Grabbed gaps file for '{no_extension}', {dir_entry.name}."
                            )
                except tarfile.ReadError:
                    pass

            # GCD Files (one per run)
            # Ex. Level2_IC86.2017_data_Run00130484_0101_71_375_GCD.i3.zst
            elif "GCD" in dir_entry.name:
                run = real.data_exp.DataExpI3FileMetadata.parse_run_number(
                    dir_entry.name
                )
                gcd_files[str(run)] = dir_entry.path
                logging.debug(f"Grabbed GCD file for run {run}, {dir_entry.name}.")

        self.real_l2_dir_metadata["dir_meta_xml"] = dir_meta_xml if dir_meta_xml else {}
        self.real_l2_dir_metadata["gaps_files"] = gaps_files
        self.real_l2_dir_metadata["gcd_files"] = gcd_files

    def _new_file_real(self, filepath: str) -> basic.BasicFileMetadata:
        """Return different metadata-file objects for `/data/exp/` files.

        Factory method.
        """
        file = utils.FileInfo(filepath)

        # L2
        if real.l2.L2FileMetadata.is_valid_filename(file.name):
            # get directory's metadata
            file_dir_path = os.path.dirname(os.path.abspath(file.path))
            if (not self.real_l2_dir_metadata) or (file_dir_path != self.dir_path):
                self.dir_path = file_dir_path
                self._real_prep_l2_dir_metadata()
            try:
                no_extension = file.name.split(".i3")[0]
                gaps = self.real_l2_dir_metadata["gaps_files"][no_extension]
            except KeyError:
                gaps = {}
            try:
                run = real.data_exp.DataExpI3FileMetadata.parse_run_number(file.name)
                gcd = self.real_l2_dir_metadata["gcd_files"][str(run)]
            except KeyError:
                gcd = ""
            logging.debug(f"Gathering L2 metadata for {file.name}...")
            return real.l2.L2FileMetadata(
                file, self.site, self.real_l2_dir_metadata["dir_meta_xml"], gaps, gcd
            )
        # PFFilt
        if real.pffilt.PFFiltFileMetadata.is_valid_filename(file.name):
            logging.debug(f"Gathering PFFilt metadata for {file.name}...")
            return real.pffilt.PFFiltFileMetadata(file, self.site)
        # PFDST
        if real.pfdst.PFDSTFileMetadata.is_valid_filename(file.name):
            logging.debug(f"Gathering PFDST metadata for {file.name}...")
            return real.pfdst.PFDSTFileMetadata(file, self.site)
        # PFRaw
        if real.pfraw.PFRawFileMetadata.is_valid_filename(file.name):
            logging.debug(f"Gathering PFRaw metadata for {file.name}...")
            return real.pfraw.PFRawFileMetadata(file, self.site)
        #
        # If no match, fall-through to basic.BasicFileMetadata...
        return self._new_file_basic_only(filepath)

    def _new_file_simulation(self, filepath: str) -> basic.BasicFileMetadata:
        """Return different metadata-file objects for `/data/sim/` files.

        Factory method.
        """
        file = utils.FileInfo(filepath)

        # read-in regex file
        if not self.sim_regexes:
            for regex in simulation.filename_patterns.regex_patterns:
                self.sim_regexes.append(re.compile(regex))

        if not self.iceprod_conn:
            raise Exception("Missing IceProd Connection Instance.")

        if DataSimI3FileMetadata.is_valid_filename(file.name):
            logging.debug(f"Gathering Sim metadata for {file.name}...")
            return DataSimI3FileMetadata(
                file, self.site, self.sim_regexes, self.iceprod_conn
            )

        return self._new_file_basic_only(filepath)

    @staticmethod
    def _is_data_sim_filepath(filepath: str) -> bool:
        return filepath.startswith("/data/sim/")

    @staticmethod
    def _is_data_exp_filepath(filepath: str) -> bool:
        return filepath.startswith("/data/exp/")

    def new_file(self, filepath: str) -> basic.BasicFileMetadata:
        """Return different metadata-file objects for files.

        Factory method.
        """
        if self.basic_only:
            return self._new_file_basic_only(filepath)

        elif MetadataManager._is_data_sim_filepath(filepath):
            return self._new_file_simulation(filepath)

        elif MetadataManager._is_data_exp_filepath(filepath):
            return self._new_file_real(filepath)

        else:
            raise RuntimeError(
                f"Unaccounted for filepath type: {filepath}. "
                "Run with --basic-only for basic metadata collection."
            )
